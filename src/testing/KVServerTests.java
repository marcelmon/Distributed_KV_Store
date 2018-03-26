package testing;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import common.comms.CommMod;
import common.comms.IntraServerCommsHelper;
import common.messages.KVMessage;
import common.messages.Message.StatusType;
import junit.framework.TestCase;
import app_kvServer.*;
import app_kvServer.ICache.KeyDoesntExistException;
import client.KVStore;

public class KVServerTests extends TestCase {
	@Test
	public void setUp() throws Exception {
//		IntraServerCommsHelper.ResetZookeeper("localhost:2181");
	}
	
	@Test
	public void testForcePut() throws Exception {
		int port = 8215;
		KVServer s1 = new KVServer("localhost", port, "localhost", 2181, 10, "LFU");
		KVServer s2 = new KVServer("localhost", port+1, "localhost", 2181, 10, "LFU");

		s1.run();
		s2.run();
		
		s1.clearCache();
		s1.clearStorage();
		s2.clearCache();
		s2.clearStorage();
		
		s1.start();
		s2.start();
		
		Thread.sleep(200); // allow the zk hasher update to propogate to s1

		CommMod comm = new CommMod();		
		comm.Connect("localhost", port);
		
		// Check that a regular put returns SERVER_NOT_RESPONSIBLE:
		KVMessage resp0 = comm.SendMessage(new KVMessage(StatusType.PUT, "localhost:" + port, "value"));
		assertTrue(resp0 != null);
		System.out.println(resp0.getStatus());
		assertTrue(resp0.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));
		boolean keyExists = true;
		try {
			s1.getKV("localhost:" + port);
		} catch (KeyDoesntExistException e) {
			keyExists = false;
		}
		assertFalse(keyExists);
		
		// Check that a force put works:
		KVMessage resp1 = comm.SendMessage(new KVMessage(StatusType.FORCE_PUT, "localhost:" + port, "value"));
		assertTrue(resp1.getStatus().equals(StatusType.PUT_SUCCESS));
		assertTrue(s1.getKV("localhost:" + port).equals("value"));	
	}
	
	@Test
	public void testPutForwarding() throws Exception {
		int port = 8225;
		KVServer s1 = new KVServer("localhost", port, "localhost", 2181, 10, "LFU");
		KVServer s2 = new KVServer("localhost", port+1, "localhost", 2181, 10, "LFU");

		s1.run();
		s2.run();
		
		s1.clearCache();
		s1.clearStorage();
		s2.clearCache();
		s2.clearStorage();
		
		s1.start();
		s2.start();
		
		// Put:
		String key = "mykey";
		String value = "myvalue";
		
		// Check it isn't already present:
		boolean keyExists = true;
		try {
			s1.getKV(key);
		} catch (KeyDoesntExistException e) {
			keyExists = false;
		}
		assertFalse(keyExists);
		
		keyExists = false;
		try {
			s2.getKV(key);
		} catch (KeyDoesntExistException e) {
			keyExists = false;
		}
		assertFalse(keyExists);
		
		// Issue a put. This will hit one server and should be forwarded to the other:
		KVStore store = new KVStore("localhost", port);
		store.put(key, value);
		
		// Give it some time:
		Thread.sleep(250);
		
		try {
			assertTrue(s1.getKV(key).equals(value));
			assertTrue(s2.getKV(key).equals(value));
		} catch (KeyDoesntExistException e) {
			fail("Key doesn't exist");
		}
	}
	
	@Test
	public void testPutForwardingLimited() throws Exception {
		int port = 8230;
		final int N = 5;
		final int REPLICATION = 2; // we expect a put to be forwarded to this many redundant servers
		
		// Create servers:
		List<KVServer> servers = new ArrayList<KVServer>();
		for (int i = 0; i < N; i++) {
			servers.add(new KVServer("localhost", port+i, "localhost", 2181, 10, "LFU"));
		}
		
		// Setup servers:
		for (KVServer s : servers) {
			s.run();
			s.clearCache();
			s.clearStorage();
			s.start();
		}

		// Put:
		String key = "mykey";
		String value = "myvalue";
		
		// Check it isn't already present:
		for (KVServer s : servers) {
			boolean keyExists = true;
			try {
				s.getKV(key);
			} catch (KeyDoesntExistException e) {
				keyExists = false;
			}
			assertFalse(keyExists);
		}
		
		// Issue a put. This will hit one server and should be forwarded to 2 others:
		KVStore store = new KVStore("localhost", port);
		store.put(key, value);
		
		// Give it some time:
		Thread.sleep(250);
		
		// Check it is in exactly 3 total:
		int instances = 0;
		for (KVServer s : servers) {
			boolean keyExists = true;
			try {
				s.getKV(key);
			} catch (KeyDoesntExistException e) {
				keyExists = false;
			}
			
			if (keyExists) instances++;
		}
		System.out.println("instances: " + instances);
		assertTrue(instances == REPLICATION + 1);
	}
}
