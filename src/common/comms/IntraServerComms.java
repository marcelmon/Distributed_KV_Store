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
	protected String me = null;
	protected IIntraServerCommsListener listener = null;
	protected IConsistentHasher hasher = null;
	
	public IntraServerComms(String zkAddr) throws Exception {		
		zk = new ZooKeeper(zkAddr, 100, this);
		
		hasher = new ConsistentHasher();
		
		// If cluster group doesn't exist, create it
		if (zk.exists(clusterGroup, false) == null) {
			zk.create(clusterGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// Set up server watcher:
		List<String> servers = zk.getChildren(clusterGroup, true);		
		hasher.fromServerListString(servers);

		//TODO set up rpc watcher
	}
	
	@Override
	public void close() throws Exception {
		zk.close();
	}
	
	protected void finalize() throws Exception {
		close();
	}
	
	@Override
	public boolean call(String target, RPCMethod method, String... args) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void register(IIntraServerCommsListener server) {
		listener = server;
		System.out.println("Sent to listener initial");
		listener.consistentHasherUpdated(hasher); // pass it the current hasher
	}

	@Override
	public void addServer(String hostname, Integer port) throws ServerExistsException, Exception {
		if (me != null) {
			throw new ServerExistsException("Multiple calls to addServer() detected");
		}
		try {
			String _me = hostname + ":" + port;
			String node = clusterGroup + "/" + _me;
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
			me = _me;
		} catch (NodeExistsException e) {
			throw new ServerExistsException(hostname + ":" + port);
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
		    // TODO RPC
//		    // Listen for RPCs:
//			} else if (event.getPath().equals(rpcGroup)) {
//				
			} else {
				// TODO log error
				System.out.println("Unknown zookeeper event occurred");
			}
		} else {
			System.out.println("Dropped zk event");
		}
	}

}
