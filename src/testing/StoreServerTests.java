package testing;

import java.util.Iterator;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;

import app_kvServer.*;
import client.*;
import common.messages.KVMessage;

public class StoreServerTests extends TestCase {
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
}
