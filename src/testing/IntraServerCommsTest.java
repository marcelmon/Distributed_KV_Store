package testing;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;

import common.comms.IConsistentHasher;
import common.comms.IConsistentHasher.ServerRecord;
import common.comms.IIntraServerComms.RPCMethod;
import common.comms.IIntraServerCommsListener;
import common.comms.IntraServerComms;
import common.comms.IntraServerComms.RPCRecord;
import junit.framework.TestCase;

public class IntraServerCommsTest extends TestCase implements Watcher {
	private final String zkAddr = "127.0.0.1:2181";
	private final String clusterGroup = "/cluster";
	private final String rpcGroup = "/rpc";
	
	protected class RPCListener implements IIntraServerCommsListener {
		public int startCnt = 0;
		public int stopCnt = 0;
		public int lockCnt = 0;
		public int unlockCnt = 0;
		public int movedataCnt = 0;
		
		public void reset() {
			startCnt = 0;
			stopCnt = 0;
			lockCnt = 0;
			unlockCnt = 0;
			movedataCnt = 0;
		}

		@Override
		public void consistentHasherUpdated(IConsistentHasher hasher) { }

		@Override
		public void start() {
			startCnt++;
		}

		@Override
		public void stop() {
			stopCnt++;
		}

		@Override
		public void lockWrite() {
			lockCnt++;
		}

		@Override
		public void unlockWrite() {
			unlockCnt++;
		}

		@Override
		public boolean moveData(String[] hashRange, String targetName) throws Exception {
			movedataCnt++;
			return false;
		}
		
	}
	
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
		List<String> servernodes = zk.getChildren(clusterGroup, false);
		for (String n : servernodes) {
			try {
				Stat stat = zk.exists(clusterGroup + "/" + n, false);
				zk.delete(clusterGroup + "/" + n, stat.getVersion());
			} catch (KeeperException e) {
				if (e.code() != KeeperException.Code.NONODE) throw e;
			}
		}
		
		// Create the rpc group if it doesn't exist:
		try {
			zk.create(rpcGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (NodeExistsException e) {
			// don't care
		}
		
		// Clear it out:
		List<String> rpcnode = zk.getChildren(rpcGroup, false);
		for (String n : rpcnode) {
			try {
				Stat stat = zk.exists(rpcGroup + "/" + n, false);
				zk.delete(rpcGroup + "/" + n, stat.getVersion());
			} catch (KeeperException e) {
				if (e.code() != KeeperException.Code.NONODE) throw e;
			}
		}
		
		Thread.sleep(200); // give zookeeper time to execute
	}
	
	@Test
	public void testAddServer() throws Exception {
		// Init zookeeper client and get initial state:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		List<String> nodes_before = zk.getChildren("/cluster", false);
		assertTrue(nodes_before.size() == 0);
		
		// Register with ISC:
		IntraServerComms isc = new IntraServerComms(zkAddr, "a", 3000);
		isc.addServer();
		
		// Check new state:
		List<String> nodes_after = zk.getChildren("/cluster", false);
		assertTrue(nodes_after.size() == 1);
		assertTrue(nodes_after.get(0).equals("a:3000"));
	}

	@Test
	public void testRemoveServer() throws Exception {
		// Init zookeeper client and get initial state:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		List<String> nodes_before = zk.getChildren("/cluster", false);
		assertTrue(nodes_before.size() == 0);
		
		// Register with ISC:
		IntraServerComms isc = new IntraServerComms(zkAddr, "b", 3000);
		isc.addServer();
		
		// Check new state:
		List<String> nodes_after_add = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_add.size() == 1);
		assertTrue(nodes_after_add.get(0).equals("b:3000"));
		
		// Unregister:
		isc.removeServer();
		
		// Check new state:
		List<String> nodes_after_remove = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_remove.size() == 0);
	}
	
	@Test
	public void testServerEphemeral() throws Exception {
		// Init zookeeper client and get initial state:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		List<String> nodes_before = zk.getChildren("/cluster", false);
		assertTrue(nodes_before.size() == 0);
		
		// Register with ISC:
		IntraServerComms isc = new IntraServerComms(zkAddr, "c", 3000);
		isc.addServer();
		
		// Check new state:
		List<String> nodes_after_add = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_add.size() == 1);
		assertTrue(nodes_after_add.get(0).equals("c:3000"));
		
		// Kill the ISC (and therefore the internal zookeeper client) and wait a second:
		isc.close();
		isc = null;
//		Thread.sleep(30000);
		
		// Check new state:
		List<String> nodes_after_remove = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_remove.size() == 0);
	}
	
	@Test
	public void testHasherUpdateAddition() throws Exception {
		// Let's set up a single pre-existing server:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		zk.create(clusterGroup + "/initial:2468", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		// Create our comms:
		HasherUpdateListener listener = new HasherUpdateListener();	
		IntraServerComms comms = new IntraServerComms(zkAddr, "d", 1234);
		comms.register(listener);
		
		// Check the existing server is present:
		ServerRecord[] servers_initial = listener.hasher.getServerList();
		assertTrue(servers_initial.length == 1);
		assertTrue(servers_initial[0].hostname.equals("initial"));
		assertTrue(servers_initial[0].port.equals(2468));
		
		// Register our server:
		comms.addServer();
		Thread.sleep(100); // give zookeeper some time to update

		// Check the new server is present:
		ServerRecord[] servers_subsequent = listener.hasher.getServerList();
//		for (ServerRecord r : servers_subsequent) {
//			System.out.println(r.hostname);
//		}
		assertTrue(servers_subsequent.length == 2);
		assertTrue(servers_subsequent[0].hostname.equals("initial"));
		assertTrue(servers_subsequent[0].port.equals(2468));
		assertTrue(servers_subsequent[1].hostname.equals("d"));
		assertTrue(servers_subsequent[1].port.equals(1234));
	}
	
	@Test
	public void testHasherUpdateRemoval() throws Exception {
		// Let's set up a single pre-existing server:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		zk.create(clusterGroup + "/initial:2468", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		// Create our comms:
		HasherUpdateListener listener = new HasherUpdateListener();	
		IntraServerComms comms = new IntraServerComms(zkAddr, "e", 1234);
		comms.register(listener);
		
		// Check the existing server is present:
		ServerRecord[] servers_initial = listener.hasher.getServerList();
		assertTrue(servers_initial.length == 1);
		assertTrue(servers_initial[0].hostname.equals("initial"));
		assertTrue(servers_initial[0].port.equals(2468));
		
		// Register our server:
		comms.addServer();
		Thread.sleep(1000); // give zookeeper some time to update

		// Check the new server is present:
		ServerRecord[] servers_subsequent = listener.hasher.getServerList();
//		for (ServerRecord r : servers_subsequent) {
//			System.out.println(r.hostname);
//		}
		assertTrue(servers_subsequent.length == 2);
		assertTrue(servers_subsequent[0].hostname.equals("initial"));
		assertTrue(servers_subsequent[0].port.equals(2468));
		assertTrue(servers_subsequent[1].hostname.equals("e"));
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
	
	@Test
	public void testRPCSend() throws Exception {
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// Create ISC:
		RPCListener listener = new RPCListener();
		IntraServerComms comms = new IntraServerComms(zkAddr, "f", 1234);
		comms.register(listener);
		
		// Ensure no calls already in queue:
		assertTrue(zk.getChildren(rpcGroup, false).size() == 0);
		
		// Make call:
		comms.call("f:1234", RPCMethod.Start);
		
		// Ensure call is in the queue:
		List<String> queue = zk.getChildren(rpcGroup, false);
		assertTrue(queue.size() == 1);
		String[] splnode = queue.get(0).split("-"); 
		assertTrue(splnode.length == 2);
		System.out.println(splnode[0]);
		assertTrue(splnode[0].equals("f:1234"));		
	}
	
	@Test
	public void testRPCReceive() throws Exception {
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// Create ISC:
		RPCListener listener = new RPCListener();
		IntraServerComms comms = new IntraServerComms(zkAddr, "g", 1234);
		comms.register(listener);
		
		// Ensure no calls already in queue:
		assertTrue(zk.getChildren(rpcGroup, false).size() == 0);
		
		// Place call in the queue:
		RPCRecord rec = new RPCRecord(RPCMethod.Start);
		zk.create(rpcGroup + "/g:1234-", rec.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		Thread.sleep(100);
		
		// Ensure call received:
		System.out.println("startCnt=" + listener.startCnt);
		assertTrue(listener.startCnt == 1);
		assertTrue(listener.stopCnt == 0);
		assertTrue(listener.lockCnt == 0);
		assertTrue(listener.unlockCnt == 0);
		assertTrue(listener.movedataCnt == 0);
	}
	
	@Test
	public void testRPCCleanup() throws Exception {
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// Create ISC:
		RPCListener listener = new RPCListener();
		IntraServerComms comms = new IntraServerComms(zkAddr, "h", 1234);
		comms.register(listener);
		
		// Ensure no calls already in queue:
		assertTrue(zk.getChildren(rpcGroup, false).size() == 0);
		
		// Place call in the queue:
		RPCRecord rec = new RPCRecord(RPCMethod.Start);
		String actual = zk.create(rpcGroup + "/h:1234-", rec.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		Thread.sleep(500);
		
		// Ensure call received:
		assertTrue(listener.startCnt == 1);
		assertTrue(listener.stopCnt == 0);
		assertTrue(listener.lockCnt == 0);
		assertTrue(listener.unlockCnt == 0);
		assertTrue(listener.movedataCnt == 0);
		
		// Ensure call isn't present anymore:
		Thread.sleep(100);
		assertTrue(zk.exists(actual, false) == null);
	}
}
