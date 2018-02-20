package testing;

import java.util.Arrays;

import org.junit.Test;
import common.messages.*;
import common.messages.KVMessage.StatusType;
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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.*;

public class ZookeeperExtTest extends TestCase implements Watcher {
	protected static Logger logger = Logger.getRootLogger();
	protected final String addr = "127.0.0.1:2181";
	
	@Test
	public void testEphemeral() throws Exception {
		final String key = "/bluh";
		final String value = "mydata";
		
		// Client 1:
		ZooKeeper zoo = new ZooKeeper(addr, 100, this);
		
		// Create key:
		assertTrue(zoo.create(key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) != null);
		assertTrue(zoo.exists(key, false) != null);
		
		// Close client 1:
		zoo.close();
		zoo = null;		
		
		// Give Zookeeper some time to kill the znode:
		Thread.sleep(1000);
		
		// Open a new client:
		zoo = new ZooKeeper(addr, 100, this);
		
		// znode should be gone:
		assertFalse(zoo.exists(key, false) != null);
		
	}
	
	@Test
	public void testSequential() throws Exception {
		final String group = "/grpa";
		final String key = "seq-";
		final String value = "mydata";
		
		ZooKeeper zoo = new ZooKeeper(addr, 100, this);
		
		try {
			assertTrue(zoo.create(group, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) != null);
		} catch (KeeperException.NodeExistsException e) {
			// don't care
		}
		
		List<String> children_before = zoo.getChildren(group, false);
		assertTrue(zoo.create(group + "/" + key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL) != null);
		List<String> children_after = zoo.getChildren(group, false);
		
		assertTrue(children_before.size() == 0);
		assertTrue(children_after.size() == 1);
		
		String key_result  = children_after.get(0);
		assertTrue(key_result.contains(key));
		
		String suffix = key_result.substring(key.length(), key_result.length());
		assertTrue(suffix.length() != 0);  // there is a suffix which isn't empty
		boolean isInteger = true;
		try {
			Integer.parseInt(suffix);
		} catch (NumberFormatException e) {
			isInteger = false;
		}
		assertTrue(isInteger);
	}

	@Override
	public void process(WatchedEvent event) {
		// do nothing
	}
}
