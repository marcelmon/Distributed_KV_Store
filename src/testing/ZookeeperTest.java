package testing;

import java.util.Arrays;

import org.junit.Test;
import common.messages.*;
import common.messages.Message.StatusType;
import junit.framework.TestCase;
import junit.runner.Version;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import java.lang.Thread;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.*;

public class ZookeeperTest extends TestCase implements Watcher {
	protected static Logger logger = Logger.getRootLogger();
	protected final String addr = "127.0.0.1:2181";
	protected List<WatchedEvent> eventQueue = new ArrayList<WatchedEvent>();
	protected ZooKeeper zk = null;
	
	@Test
	public void testInit() throws Exception {
		new ZooKeeper(addr, 1000, this);
	}
	
	@Test
	public void testCreate() throws Exception {
		final String key = "/asbsd";
		final String value = "mydata";
		
		ZooKeeper zoo = new ZooKeeper(addr, 1000, this);
		assertTrue(zoo.create(key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) != null);
		Stat stat = null;
		byte[] data = zoo.getData(key, false, stat);		
		assertTrue(Arrays.equals(data, value.getBytes()));
	}
	
	@Test
	public void testAddNotify() throws Exception {
		final String key = "/mynovelkey2";
		
		ZooKeeper zoo = new ZooKeeper(addr, 1000, this);
		assertTrue(zoo.exists(key, true) == null);
		eventQueue.clear();
		assertTrue(zoo.create(key, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) != null);
		Thread.sleep(500); // some time for zookeeper to update
		assertTrue(eventQueue.size() == 1);
		assertTrue(eventQueue.get(0).getType().equals(Watcher.Event.EventType.NodeCreated));
		assertTrue(eventQueue.get(0).getPath().equals(key));
	}
	
	@Test
	public void testDeleteNotify() throws Exception {
		final String key = "/mynovelkey";
		
		ZooKeeper zoo = new ZooKeeper(addr, 1000, this);
		assertTrue(zoo.create(key, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) != null);
		eventQueue.clear();
		assertTrue(zoo.exists(key, true) != null);
		zoo.delete(key, -1);
		Thread.sleep(500); // some time for zookeeper to update
		assertTrue(eventQueue.size() == 1);
		assertTrue(eventQueue.get(0).getType().equals(Watcher.Event.EventType.NodeDeleted));
		assertTrue(eventQueue.get(0).getPath().equals(key));
	}
	
	@Test
	public void testAddChildNotify() throws Exception {
		final String parent = "/mynovelparent";
		final String child = "child";
		
		ZooKeeper zoo = new ZooKeeper(addr, 1000, this);
		try {
			assertTrue(zoo.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) != null);
		} catch (KeeperException e) {
			if (e.code() != Code.NODEEXISTS) throw e; // don't care if it already exists
		}
		assertTrue(zoo.getChildren(parent, true).size() == 0);
		eventQueue.clear();
		assertTrue(zoo.create(
				parent + "/" + child, 
				new byte[0], 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL) != null);
		Thread.sleep(100); // some time for zookeeper to update
		assertTrue(eventQueue.size() == 1);
		assertTrue(eventQueue.get(0).getType().equals(Watcher.Event.EventType.NodeChildrenChanged));
		assertTrue(eventQueue.get(0).getPath().equals(parent));
		assertTrue(zoo.getChildren(parent, false).size() == 1);
		assertTrue(zoo.getChildren(parent, false).get(0).equals(child));
	}
	
	@Test
	public void testDeleteChildNotify() throws Exception {
		final String parent = "/mynovelparent2";
		final String child = "child";
		
		ZooKeeper zoo = new ZooKeeper(addr, 1000, this);
		zk = zoo;
		try {
			assertTrue(zoo.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) != null);
		} catch (KeeperException e) {
			if (e.code() != Code.NODEEXISTS) throw e; // don't care if it already exists
		}
		assertTrue(zoo.getChildren(parent, true).size() == 0);
		assertTrue(zoo.create(
				parent + "/" + child, 
				new byte[0], 
				ZooDefs.Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL) != null);
		assertTrue(zoo.exists(parent + "/" + child, false) != null); // exists now
		zoo.getChildren(parent, true); // must rewatch since we were notified about above creation
		eventQueue.clear();
		zoo.delete(parent + "/" + child, -1);
		assertTrue(zoo.exists(parent + "/" + child, false) == null); // doesn't exist now
		Thread.sleep(100); // some time for zookeeper to update
		assertTrue(eventQueue.size() == 1);
		assertTrue(eventQueue.get(0).getType().equals(Watcher.Event.EventType.NodeChildrenChanged));
		assertTrue(eventQueue.get(0).getPath().equals(parent));
		assertTrue(zoo.getChildren(parent, false).size() == 0);
		
		zk = null;
	}

	@Override
	public void process(WatchedEvent event) {
		eventQueue.add(event);
	}
}
