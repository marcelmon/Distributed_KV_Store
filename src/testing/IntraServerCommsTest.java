package testing;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.BeforeClass;
import org.junit.Test;

import common.comms.IConsistentHasher;
import common.comms.IConsistentHasher.ServerRecord;
import common.comms.IIntraServerCommsListener;
import common.comms.IntraServerComms;
import junit.framework.TestCase;

public class IntraServerCommsTest extends TestCase implements Watcher {
	private final String zkAddr = "127.0.0.1:2181";
	private final String clusterGroup = "/cluster";
	
	protected class HasherUpdateListener implements IIntraServerCommsListener {
		public IConsistentHasher hasher;

		@Override
		public void consistentHasherUpdated(IConsistentHasher hasher) {
			System.out.println("Listener rx");
			this.hasher = hasher;			
		}

		@Override
		public void start() {}
		@Override
		public void stop() {}
		@Override
		public void lockWrite() {}
		@Override
		public void unlockWrite() {}
		@Override
		public boolean moveData(String[] hashRange, String targetName) throws Exception {return false; }		
	}
	
	@Override
	public void process(WatchedEvent event) {
		// do nothing	
	}
	
	@Override
	public void setUp() throws Exception {
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		
		// Create the cluster group if it doesn't exist:
		try {
			zk.create(clusterGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (NodeExistsException e) {
			// don't care
		}
		
		// Clear it out:
		List<String> nodes = zk.getChildren(clusterGroup, false);
		for (String n : nodes) {
			zk.delete(clusterGroup + "/" + n, -1);
		}
	}
	
//	@Test
//	public void testAddServer() throws Exception {
//		// Init zookeeper client and get initial state:
//		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
//		List<String> nodes_before = zk.getChildren("/cluster", false);
//		assertTrue(nodes_before.size() == 0);
//		
//		// Register with ISC:
//		IntraServerComms isc = new IntraServerComms(zkAddr);
//		isc.addServer("localhost", 3000);
//		
//		// Check new state:
//		List<String> nodes_after = zk.getChildren("/cluster", false);
//		assertTrue(nodes_after.size() == 1);
//		assertTrue(nodes_after.get(0).equals("localhost:3000"));
//	}
//
//	@Test
//	public void testRemoveServer() throws Exception {
//		// Init zookeeper client and get initial state:
//		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
//		List<String> nodes_before = zk.getChildren("/cluster", false);
//		assertTrue(nodes_before.size() == 0);
//		
//		// Register with ISC:
//		IntraServerComms isc = new IntraServerComms(zkAddr);
//		isc.addServer("localhost", 3000);
//		
//		// Check new state:
//		List<String> nodes_after_add = zk.getChildren("/cluster", false);
//		assertTrue(nodes_after_add.size() == 1);
//		assertTrue(nodes_after_add.get(0).equals("localhost:3000"));
//		
//		// Unregister:
//		isc.removeServer();
//		
//		// Check new state:
//		List<String> nodes_after_remove = zk.getChildren("/cluster", false);
//		assertTrue(nodes_after_remove.size() == 0);
//	}
//	
//	@Test
//	public void testServerEphemeral() throws Exception {
//		// Init zookeeper client and get initial state:
//		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
//		List<String> nodes_before = zk.getChildren("/cluster", false);
//		assertTrue(nodes_before.size() == 0);
//		
//		// Register with ISC:
//		IntraServerComms isc = new IntraServerComms(zkAddr);
//		isc.addServer("localhost", 3000);
//		
//		// Check new state:
//		List<String> nodes_after_add = zk.getChildren("/cluster", false);
//		assertTrue(nodes_after_add.size() == 1);
//		assertTrue(nodes_after_add.get(0).equals("localhost:3000"));
//		
//		// Kill the ISC (and therefore the internal zookeeper client) and wait a second:
//		isc.close();
//		isc = null;
////		Thread.sleep(30000);
//		
//		// Check new state:
//		List<String> nodes_after_remove = zk.getChildren("/cluster", false);
//		assertTrue(nodes_after_remove.size() == 0);
//	}
	
//	@Test
//	public void testHasherUpdateAddition() throws Exception {
//		// Let's set up a single pre-existing server:
//		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
//		zk.create(clusterGroup + "/initial:2468", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//		
//		// Create our comms:
//		HasherUpdateListener listener = new HasherUpdateListener();	
//		IntraServerComms comms = new IntraServerComms(zkAddr);
//		comms.register(listener);
//		
//		// Check the existing server is present:
//		ServerRecord[] servers_initial = listener.hasher.getServerList();
//		assertTrue(servers_initial.length == 1);
//		assertTrue(servers_initial[0].hostname.equals("initial"));
//		assertTrue(servers_initial[0].port.equals(2468));
//		
//		// Register our server:
//		comms.addServer("me", 1234);
//		Thread.sleep(100); // give zookeeper some time to update
//
//		// Check the new server is present:
//		ServerRecord[] servers_subsequent = listener.hasher.getServerList();
////		for (ServerRecord r : servers_subsequent) {
////			System.out.println(r.hostname);
////		}
//		assertTrue(servers_subsequent.length == 2);
//		assertTrue(servers_subsequent[0].hostname.equals("initial"));
//		assertTrue(servers_subsequent[0].port.equals(2468));
//		assertTrue(servers_subsequent[1].hostname.equals("me"));
//		assertTrue(servers_subsequent[1].port.equals(1234));
//	}
	
	@Test
	public void testHasherUpdateRemoval() throws Exception {
		// Let's set up a single pre-existing server:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		zk.create(clusterGroup + "/initial:2468", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		// Create our comms:
		HasherUpdateListener listener = new HasherUpdateListener();	
		IntraServerComms comms = new IntraServerComms(zkAddr);
		comms.register(listener);
		
		// Check the existing server is present:
		ServerRecord[] servers_initial = listener.hasher.getServerList();
		assertTrue(servers_initial.length == 1);
		assertTrue(servers_initial[0].hostname.equals("initial"));
		assertTrue(servers_initial[0].port.equals(2468));
		
		// Register our server:
		comms.addServer("me", 1234);
		Thread.sleep(1000); // give zookeeper some time to update

		// Check the new server is present:
		ServerRecord[] servers_subsequent = listener.hasher.getServerList();
//		for (ServerRecord r : servers_subsequent) {
//			System.out.println(r.hostname);
//		}
		assertTrue(servers_subsequent.length == 2);
		assertTrue(servers_subsequent[0].hostname.equals("initial"));
		assertTrue(servers_subsequent[0].port.equals(2468));
		assertTrue(servers_subsequent[1].hostname.equals("me"));
		assertTrue(servers_subsequent[1].port.equals(1234));
		
		// Remove our server:
		comms.removeServer();
		Thread.sleep(1000);

//		// Check our server is removed:
		System.out.println("checking...");
		ServerRecord[] servers_final = null;
		for (int i = 0; i < 5 && (servers_final == null || servers_final.length != 1); i++) {
			servers_final = listener.hasher.getServerList();
			System.out.println(servers_final.length);
			for (ServerRecord r : servers_final)
				System.out.println(r.hostname);
			Thread.sleep(100);
		}
		assertTrue(servers_final.length == 1);
		assertTrue(servers_final[0].hostname.equals("initial"));
		assertTrue(servers_final[0].port.equals(2468));
	}
	
	//TODO test getting an updated consistenthasher after server addition/removal
	//TODO test rpc
}
