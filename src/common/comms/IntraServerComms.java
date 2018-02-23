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
		public Object[] args;
		
		public byte[] getBytes() {
			byte[] buffer = new byte[1];
			buffer[0] = (byte) method.ordinal();
			//TODO manage args
			return buffer;
		}
		
		protected void fromBytes(byte[] bytes) {
			method = RPCMethod.values()[bytes[0]];
			//TODO manage args
		}
		
		public RPCRecord(byte[] bytes) {
			fromBytes(bytes);
		}
		
		public RPCRecord(RPCMethod method, Object... args) {
			this.method = method;
			this.args = args;
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
	public void call(String target, RPCMethod method, Object... args) throws InvalidArgsException, Exception {
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
						//TODO 
						throw new RuntimeException("Unimplemented rpc method: movedata");
					default:
						throw new Exception("Unknown RPC method encountered");
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
