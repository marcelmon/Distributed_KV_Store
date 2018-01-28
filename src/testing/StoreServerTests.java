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
			System.out.println("Resp: " + resp.getStatus());
			System.out.println("Resp: " + resp.getKey());
			System.out.println("Resp: " + resp.getValue());
			assertTrue(resp.getValue().equals("b"));
		} catch (Exception e) {
			fail("Unknown error: " + e.getMessage());
		}
	}
}
