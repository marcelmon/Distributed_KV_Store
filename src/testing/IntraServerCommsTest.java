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

import common.comms.IntraServerComms;
import junit.framework.TestCase;

public class IntraServerCommsTest extends TestCase implements Watcher {
	private final String zkAddr = "127.0.0.1:2181";
	private final String clusterGroup = "/cluster";
	
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
	
	@Test
	public void testAddServer() throws Exception {
		// Init zookeeper client and get initial state:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		List<String> nodes_before = zk.getChildren("/cluster", false);
		assertTrue(nodes_before.size() == 0);
		
		// Register with ISC:
		IntraServerComms isc = new IntraServerComms(zkAddr);
		isc.addServer("localhost", 3000);
		
		// Check new state:
		List<String> nodes_after = zk.getChildren("/cluster", false);
		assertTrue(nodes_after.size() == 1);
		assertTrue(nodes_after.get(0).equals("localhost:3000"));
	}

	@Test
	public void testRemoveServer() throws Exception {
		// Init zookeeper client and get initial state:
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		List<String> nodes_before = zk.getChildren("/cluster", false);
		assertTrue(nodes_before.size() == 0);
		
		// Register with ISC:
		IntraServerComms isc = new IntraServerComms(zkAddr);
		isc.addServer("localhost", 3000);
		
		// Check new state:
		List<String> nodes_after_add = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_add.size() == 1);
		assertTrue(nodes_after_add.get(0).equals("localhost:3000"));
		
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
		IntraServerComms isc = new IntraServerComms(zkAddr);
		isc.addServer("localhost", 3000);
		
		// Check new state:
		List<String> nodes_after_add = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_add.size() == 1);
		assertTrue(nodes_after_add.get(0).equals("localhost:3000"));
		
		// Kill the ISC (and therefore the internal zookeeper client) and wait a second:
		isc.close();
		isc = null;
//		Thread.sleep(30000);
		
		// Check new state:
		List<String> nodes_after_remove = zk.getChildren("/cluster", false);
		assertTrue(nodes_after_remove.size() == 0);
	}
	
	//TODO test rpc
}
