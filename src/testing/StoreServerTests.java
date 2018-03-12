package testing;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;

import client.*;
import common.messages.KVMessage;
import common.messages.Message.StatusType;

public class StoreServerTests extends TestCase {
	@Override
	public void setUp() throws Exception {
		// Make sure to clear the physical storage:
		KVServer serv = new KVServer("localhost", 3000, "localhost", 2181, 10, "LFU"); //TODO fix when zookeeper implemented
		serv.run();
		serv.clearStorage();
		serv.close();
		serv = null;
	}
	
	@Test
	public void testInsert() {
		try {
			System.out.println("HERE 0.1\n\n\n\n\n");
			KVServer serv = new KVServer("localhost", 12343, "localhost", 2181, 10, "FIFO"); //TODO fix when zookeeper implemented
			serv.run();
			serv.clearStorage();
			serv.start();
			Thread.sleep(300);
			KVStore store = new KVStore("localhost", 12343);
			store.put("a", "b");
			KVMessage resp = store.get("a");
			// System.out.println("Resp: " + resp.getStatus());
			// System.out.println("Resp: " + resp.getKey());
			// System.out.println("Resp: " + resp.getValue());
			assertTrue(resp.getValue().equals("b"));

			serv.close();
			serv = null;
		} catch (Exception e) {
			System.out.println("testInsert() exception " + e.getMessage() + e.getMessage());
			fail("Unknown error: " + e.getMessage());
		}
	}
	
	@Test
	public void testMultiInsert() {
		final int port = 12342;
		KVServer serv = null;
		try {
			serv = new KVServer("localhost", port, "localhost", 2181, 10, "LFU"); //TODO fix when zookeeper implemented
			
			serv.run(); //TODO fix when zookeeper implemented
			serv.start();
			Thread.sleep(300);
		} catch (Exception e) {
			fail("Failed to run server: " + e.getMessage());
		}
		
		// Connect stores to server:
		final int N = 5;
		KVStore[] stores = new KVStore[N];
		for (int i = 0; i < N; i++) {
			stores[i] = new KVStore("localhost", port);
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

		serv.close();
		serv = null;
	}
	
	@Test
	public void testGetNotPresent() throws Exception {
		final int port = 2600;
		
		// Generate a server:
		KVServer server0 = new KVServer("localhost", port, "localhost", 2181, 10, "LFU");
		server0.run(); // in new thread
		server0.start();
		Thread.sleep(300);
		assertFalse(server0.inCache("A"));
		assertFalse(server0.inStorage("A"));
		
		// Store reads from server:
		KVStore store0 = new KVStore("localhost", port);
		KVMessage resp = store0.get("A");
		store0.disconnect();
		
		// Correct message:
		KVMessage exp = new KVMessage(StatusType.GET_ERROR, "A", null);
		assertFalse(resp == null);
		assertTrue(resp.equals(exp));

		server0.close();
		server0 = null;
	}
	
	@Test
	public void testKill() throws Exception {
		final int port = 2500;
		
		// Generate a server:
		KVServer server0 = new KVServer("localhost", port, "localhost", 2181, 10, "LFU"); //TODO fix when zookeeper implemented
		server0.run(); // in new thread
		server0.start();
		Thread.sleep(300);

		// Store writes to server:
		KVStore store0 = new KVStore("localhost", port);
		store0.put("A", "B");
		store0.disconnect();
		
		// Check that the server has the value in cache but not in storage:
		assertTrue(server0.inCache("A"));
		assertFalse(server0.inStorage("A"));
		
		// Kill the server:
		server0.close();
		server0.kill();
		server0 = null;
		
		// Generate a new server (which should have the same db)
		// Notably, the original server may still be on port so we increment
		KVServer server1 = new KVServer("localhost", port+1, "localhost", 2181, 10, "LFU"); //TODO fix when zookeeper implemented
		server1.run();
		
		server1.start();
		Thread.sleep(300);
		// Store reads from server:
		KVStore store1 = new KVStore("localhost", port+1);
		KVMessage resp = store1.get("A");
		store1.disconnect();
		
		// We should have lost the value:
		assertFalse(resp == null);
		assertTrue(resp.getStatus().equals(StatusType.GET_ERROR));


		server1.close();
		server1 = null;
	}
	
	@Test
	public void testClose() throws Exception {
		final int port = 2100;
		
		// Generate a server:
		KVServer server0 = new KVServer("localhost", port, "localhost", 2181, 10, "LFU"); //TODO fix when zookeeper implemented
		server0.run(); // in new thread
		server0.start();
		Thread.sleep(300);
		// Store writes to server:
		KVStore store0 = new KVStore("localhost", port);
		store0.put("A", "B");
		
		Thread.sleep(100);
		
		KVMessage get11 = store0.get("A");
		assertTrue(get11.getStatus().equals(StatusType.GET_SUCCESS));
		assertTrue(get11.getValue().equals("B"));

		store0.disconnect();
		// Check that the server has the value in cache but not in storage:
		assertTrue(server0.inCache("A"));
		assertFalse(server0.inStorage("A"));
		
		// Kill the server:
		server0.close();
		server0 = null;
		Thread.sleep(100);
		
		// Generate a new server (which should have the same db)
		// Notably, the original server may still be on port so we increment
		// EDIT: using same port because directory is data_dir_localhost:port
		KVServer server1 = new KVServer("localhost", port, "localhost", 2181, 10, "LFU"); //TODO fix when zookeeper implemented
		server1.run();
		server1.start();
		Thread.sleep(300);
		// Store reads from server:
		KVStore store1 = new KVStore("localhost", port);
		KVMessage resp = store1.get("A");
		store1.disconnect();
		
		// We should *NOT* have lost the value:
		assertFalse(resp == null);
		KVMessage exp = new KVMessage(StatusType.GET_SUCCESS, "A", "B");
		assertTrue(resp.equals(exp));

		server1.close();
		server1 = null;
	}
}
