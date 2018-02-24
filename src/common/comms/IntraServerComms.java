package common.comms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import common.comms.IConsistentHasher.ServerRecord;
import common.comms.IConsistentHasher.StringFormatException;

public class IntraServerComms implements IIntraServerComms, Watcher {
	protected ZooKeeper zk = null;
	protected final String clusterGroup = "/cluster";
	protected final String rpcGroup = "/rpc";	
	protected String me = null;
	protected IIntraServerCommsListener listener = null;
	protected IConsistentHasher hasher = null;
	
	public static class RPCRecord {
		public RPCMethod method;
		public String[] args;
		
		public byte[] getBytes() {
			// Format is: <RPCMethod 1> <args.length 1> <args[0].length 1> <args[0] N> ... <args[i].length 1> <args[i] N>
			
			if (args.length > 255) {
				throw new RuntimeException("More than 255 args in an RPC!"); //fatal
			}
			
			int bufferlen = 2; // RPCMethod, number of args
			for (String s : args)
				bufferlen += 1 + s.length();
			
			byte[] buffer = new byte[bufferlen];
			
			buffer[0] = (byte) method.ordinal();
			buffer[1] = (byte) args.length;
			int cursor = 2;
			for (String s : args) {
				if (s.length() > 255) {
					// should have already been truncated so fatal
					throw new RuntimeException("Unexpected long RPCRecord arg: " + s); 
				}
				buffer[cursor] = (byte) s.length();
				System.arraycopy(s.getBytes(), 0, buffer, cursor+1, s.length());
				cursor += 1 + s.length();
			}
			
			return buffer;
		}
		
		protected boolean validateArgs() {
			switch (method) {
				case Start:
				case Stop:
				case LockWrite:
				case UnlockWrite:
					return args.length == 0;
				case MoveData:
					return args.length == 3;
				default:
					// fatal because this means the application is being used incorrectly
					throw new RuntimeException("Uncountered unknown RPCMethod: " + method); 
			}
		}
		
		protected void fromBytes(byte[] bytes) throws Exception {
			if (bytes.length < 2) {
				throw new Exception("Invalid byte representation of an RPCRecord");
			}
			
			method = RPCMethod.values()[bytes[0]];
			int numargs = bytes[1];
			args = new String[numargs];
			int bytecursor = 2;
			int argcursor = 0;
			while (argcursor < numargs) {
				int arglen = bytes[bytecursor];
				byte[] rawarg = new byte[arglen];
				System.arraycopy(bytes, bytecursor+1, rawarg, 0, arglen);
				args[argcursor] = new String(rawarg);
				argcursor++;
				bytecursor += 1 + arglen;
			}
			
			if (!validateArgs()) {
				throw new Exception("Args failed to validate in fromBytes(): " + bytes.toString());
			}
		}
		
		public RPCRecord(byte[] bytes) throws Exception {
			fromBytes(bytes);
		}
		
		public RPCRecord(RPCMethod method, String... args) throws Exception {
			// TODO check that the number of arguments is correct for the method
			
			this.method = method;
			this.args = args;
			
			// Truncate args if too long:
			for (String a : this.args) {
				if (a.length() > 255) {
					a = a.substring(0,  255);
				}
			}
			
			if (!validateArgs()) {
				throw new Exception("Invalid args for call to: " + method);
			}
		}
	}
	
	public IntraServerComms(String zkAddr, String hostname, Integer port) throws Exception {	
		init(hostname, port);
		zk = new ZooKeeper(zkAddr, 100, this);
		
		hasher = new ConsistentHasher();
		
		// If cluster group doesn't exist, create it
		if (zk.exists(clusterGroup, false) == null) {
			zk.create(clusterGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// If rpc group doesn't exist, create it:
		if (zk.exists(rpcGroup, false) == null) {
			zk.create(rpcGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// Set up server watcher:
		List<String> servers = zk.getChildren(clusterGroup, true);		
		hasher.fromServerListString(servers);
		
		// Set up rpc watcher:
		processRPC();
	}
	
	@Override
	public void init(String hostname, Integer port) throws ServerExistsException {
		if (me != null) {
			throw new ServerExistsException("Multiple calls to addServer() detected");
		}
		me = hostname + ":" + port;
	}
	
	@Override
	public void close() throws Exception {
		zk.close();
	}
	
	protected void finalize() throws Exception {
		close();
	}
	
	@Override
	public void call(String target, RPCMethod method, String... args) throws InvalidArgsException, Exception {
		RPCRecord rec = new RPCRecord(method, args);
		zk.create(rpcGroup + "/" + target + "-", rec.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	}

	@Override
	public void register(IIntraServerCommsListener server) {
		listener = server;
		System.out.println("Sent to listener initial");
		listener.consistentHasherUpdated(hasher); // pass it the current hasher
	}

	@Override
	public void addServer() throws ServerExistsException, Exception {		
		try {
			String node = clusterGroup + "/" + me;
			System.out.println("Creating: " + node);
			
			// If it already exists this means a previous version of *this* server has crashed
			// and restarted (or someone isn't obeying convention). Either way we must delete
			// it so that *we* own it and its lifecycle is tied to *us*.
			if (zk.exists(node, false) != null) {
				zk.delete(node, -1);
			}
			
			// Create an ephemeral node for this server:
			zk.create(
					node,
					"".getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL);
		} catch (NodeExistsException e) {
			throw new ServerExistsException("ZK node already exists for: " + me);
		} catch (InterruptedException e) {
			throw e;
		}
		
	}

	@Override
	public void removeServer() throws NotYetRegisteredException, Exception {
		if (me == null) {
			throw new NotYetRegisteredException("Attempted to call removeServer() on a server never registered in the cluster");
		}
		try {
			System.out.println("Deleting: " + clusterGroup + "/" + me);
			zk.delete(clusterGroup + "/" + me, -1); // any version
		} catch (KeeperException e) {
			throw e;
		} catch (InterruptedException e) {
			throw e;
		}
	}
	
	protected synchronized void processRPC() throws Exception {
		if (me == null) {
			// TODO Log that we're pre init
			return;
		}
		
		// Process through the RPC queue
		List<String> calls = zk.getChildren(rpcGroup,  true); // reset watch
		StackTraceElement[] st = Thread.currentThread().getStackTrace();
		for (int i = 0; i < calls.size(); i++) {
			System.out.println("[" + Thread.currentThread().getId() + "][" + i + "]: " + calls.get(i));
		}
		for (String c : calls) {			
			// Does this refer to us?
			String[] spl = c.split("-");
			if (spl.length != 2) {
				throw new Exception("Unexpected rpc node format.");
			}
			if (spl[0].equals(me)) {
				Stat stat = new Stat();
				byte[] data = zk.getData(rpcGroup + "/" + c, false, stat);
				
				// Remove the znode for this rpc call:
				System.out.println("Exists [" + c + "]: " + (zk.exists(rpcGroup + "/" + c, false) != null));
				zk.delete(rpcGroup + "/" + c, -1);
				
				RPCRecord rec = new RPCRecord(data);
				switch (rec.method) {
					case Start:
						listener.start();
						break;
					case Stop:
						listener.stop();
						break;
					case LockWrite:
						listener.lockWrite();
						break;
					case UnlockWrite:
						listener.unlockWrite();
						break;
					case MoveData:
						if (rec.args.length != 3)  {
							throw new Exception("Invalid number of args for RPCMethod.MoveData");
						}
						listener.moveData(new String[] {rec.args[0], rec.args[1]}, rec.args[2]);
						break;
					default:
						throw new Exception("Unknown RPC method encountered: " + rec.method);
				}				
				
				// Wait until delete confirmation to ensure we don't double-dip if this method returns
				// and then (in this thread) there is another call to processRPC() before zookeeper
				// has deleted the thread
				while (zk.exists(rpcGroup + "/" + c, false) != null) {
					System.out.println("Waiting...");
					Thread.sleep(100);
					//TODO have a timeout here
				}
				System.out.println("endExists [" + c + "]: " + (zk.exists(rpcGroup + "/" + c, false) != null));
			} else {
				// TODO log that we rejected this
				System.out.println("Determined RPC \"" + c + "\" not aimed at \"" + me + "\"");
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("zk event");
		
		// Re-register for events
		// "Watches are one time triggers; if you get a watch event and you want to get notified of future changes, you must set another watch."
		// Since we don't know what kind of watch created this, let's register for all types of change
		try {
			if (event.getPath() != null) {
				zk.getChildren(event.getPath(), true); // changes to self/children
				zk.exists(event.getPath(), true); // changes to self/data
			}
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException("An unknown error occurred with zookeeper");
		}
		
		if (listener != null) {
			String[] splitPath = event.getPath().split("/");
			
			// ADDED and DELETED servers:
			if (event.getPath().equals(clusterGroup)) {
				if (event.getType() != Watcher.Event.EventType.NodeChildrenChanged) {
					throw new RuntimeException("Unexpected change to cluster group node in zookeeper");
				}
				try {
					// Something changed, so let's just read the entire current state:
					List<String> nodes = zk.getChildren(clusterGroup, false);
					for (String n : nodes)
						System.out.println("n:" + n);
					hasher.fromServerListString(nodes);
					System.out.println("Sent to listener " + event.getType().toString());
					listener.consistentHasherUpdated(hasher);
				} catch (StringFormatException e) {
					System.out.println("ERROR: " + e.getMessage());
					//TODO log this as an error - it's an external format error so life goes on					
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new RuntimeException("An unknown error occurred with zookeeper");
				} catch (KeeperException e) {
					e.printStackTrace();
					throw new RuntimeException("An unknown error occurred with zookeeper");
				}
		    // Listen for RPCs:
			} else if (event.getPath().equals(rpcGroup)) {
				try {
					processRPC();
				} catch (Exception e) {
					//TODO log error
					e.printStackTrace();
					System.out.println("Unknown error occurred processing RPCs: " + e.getMessage());
				}
			} else {
				// TODO log error
				System.out.println("Unknown zookeeper event occurred");
			}
		} else {
			System.out.println("Dropped zk event");
		}
	}

}
