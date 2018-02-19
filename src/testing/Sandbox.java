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

public class Sandbox extends TestCase implements Watcher {
	protected static Logger logger = Logger.getRootLogger();
	protected ArrayList<Byte> serverRx = new ArrayList<>();
	protected ArrayList<Byte> clientRx = new ArrayList<>();
	protected TLVMessage serverRx_msg;
	protected TLVMessage clientRx_msg;
	
	@Test
	public void testMain() throws Exception {
		ZooKeeper zoo = new ZooKeeper("127.0.0.1:2181", 1000, this);
		assertTrue(zoo.exists("/test", false) != null);
		assertTrue(zoo.create("/bluh", "mydata".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) != null);
		Thread.sleep(3000);
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event.toString());
	}
}
