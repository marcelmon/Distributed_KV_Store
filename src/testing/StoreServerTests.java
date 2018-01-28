package testing;

import java.util.Iterator;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import m1.src.app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.*;

import app_kvServer.*;
import client.*;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.TLVMessage;

public class StoreServerTests extends TestCase {
	@Override
	public void setUp() {
		// Make sure to clear the physical storage:
		KVServer serv = new KVServer(3000, 10, "LFU");
		serv.clearStorage();
		serv.close();
	}
	
	@Test
	public void testInsert() {
		try {
			new KVServer(12343, 10, "").run();
			
			KVStore store = new KVStore("localhost", 12343);
			store.connect();
			store.put("a", "b");
			KVMessage resp = store.get("a");
			// System.out.println("Resp: " + resp.getStatus());
			// System.out.println("Resp: " + resp.getKey());
			// System.out.println("Resp: " + resp.getValue());
			assertTrue(resp.getValue().equals("b"));
		} catch (Exception e) {
			fail("Unknown error: " + e.getMessage());
		}
	}
	
	@Test
	public void testMultiInsert() {
		final int port = 12342;
		try {
			new KVServer(port, 10, "").run();
		} catch (Exception e) {
			fail("Failed to run server: " + e.getMessage());
		}
		
		// Connect stores to server:
		final int N = 5;
		KVStore[] stores = new KVStore[N];
		for (int i = 0; i < N; i++) {
			stores[i] = new KVStore("localhost", port);
			try {
				stores[i].connect();
			} catch (Exception e) {
				fail("Failed to connect store " + i + " to server");
			}
		}
		
		// Use stores to put data in server:
		for (int i = 0; i < N; i++) {
			try {
				stores[i].put(Integer.toString(i), Integer.toString(i+100));
			} catch (Exception e) {
				fail("Put failed for store " + i);
			}
		}
		
		// Use stores to retrieve data put in server by other stores:
		for (int i = 0; i < N; i++) {
			int next = (i+1) % N; // circular ring next; don't want to get your own key
			try {
				KVMessage resp = stores[i].get(Integer.toString(next));
				assertTrue(resp != null);
				assertTrue(resp.getValue().equals(Integer.toString(next+100)));
			} catch (Exception e) {
				fail("Get failed for store " + i);
			}
		}
		
		// Disconnect:
		for (int i = 0; i < N; i++) {
			stores[i].disconnect();
		}
	}
	
//	@Test
//	public void testKill() throws Exception {
//		final int port = 2000;
//		
//		// Generate a server:
//		KVServer server0 = new KVServer(port, 10, "LFU");
//		server0.run(); // in new thread
//		
//		// Store writes to server:
//		KVStore store0 = new KVStore("localhost", port);
//		store0.connect();
//		store0.put("A", "B");
//		store0.disconnect();
//		
//		// Check that the server has the value in cache but not in storage:
//		assertTrue(server0.inCache("A"));
//		assertFalse(server0.inStorage("A"));
//		
//		// Kill the server:
//		server0.kill();
//		server0 = null;
//		
//		// Generate a new server (which should have the same db)
//		// Notably, the original server may still be on port so we increment
//		KVServer server1 = new KVServer(port+1, 10, "LFU");
//		server1.run();
//		
//		// Store reads from server:
//		KVStore store1 = new KVStore("localhost", port+1);
//		store1.connect();
//		KVMessage resp = store1.get("A");
//		store1.disconnect();
//		
//		// We should have lost the value:
//		assertFalse(resp == null);
//		assertTrue(resp.getStatus().equals(StatusType.GET_ERROR));
//	}
	
	@Test
	public void testClose() throws Exception {
		final int port = 2100;
		
		// Generate a server:
		KVServer server0 = new KVServer(port, 10, "LFU");
		server0.run(); // in new thread
		
		// Store writes to server:
		KVStore store0 = new KVStore("localhost", port);
		store0.connect();
		store0.put("A", "B");
		store0.disconnect();
		
		// Check that the server has the value in cache but not in storage:
		assertTrue(server0.inCache("A"));
		assertFalse(server0.inStorage("A"));
		
		// Kill the server:
		server0.close();
		server0 = null;
		
		// Generate a new server (which should have the same db)
		// Notably, the original server may still be on port so we increment
		KVServer server1 = new KVServer(port+1	, 10, "LFU");
		server1.run();
		
		// Store reads from server:
		KVStore store1 = new KVStore("localhost", port+1);
		store1.connect();
		KVMessage resp = store1.get("A");
		store1.disconnect();
		
		// We should have lost the value:
		assertFalse(resp == null);
		KVMessage exp = new TLVMessage(StatusType.GET_SUCCESS, "A", "B");
		assertTrue(resp.equals(exp));
	}
}
