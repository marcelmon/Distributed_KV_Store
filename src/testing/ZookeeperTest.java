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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.*;

public class ZookeeperTest extends TestCase implements Watcher {
	protected static Logger logger = Logger.getRootLogger();
	protected final String addr = "127.0.0.1:2181";
	
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

	@Override
	public void process(WatchedEvent event) {
		// do nothing
	}
}
