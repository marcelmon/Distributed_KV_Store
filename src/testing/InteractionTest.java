package testing;

import org.junit.Test;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import common.messages.KVMessage;
import common.messages.Message.StatusType;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	protected KVServer server;
	
	@Override
	public void setUp() throws Exception {
		int port = 50000;
		server = new KVServer("localhost", port, "localhost", 2181, 10, "LFU"); // TODO put proper args when zookeeper implemented
		kvClient = new KVStore("localhost", port);
		server.run();
		server.clearStorage();
		server.start();
		Thread.sleep(200);
	}

	@Override
	public void tearDown() {
		kvClient.disconnect();
		server.close();
		server = null;
	}
	
	
	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		if(response.getStatus() != StatusType.PUT_SUCCESS){
			System.out.println("PUT STATUS IS : " + response.getStatus() + "\n\n====\n=====\n====\n\n\n");
		}
		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
		// assertTrue(response.getValue().equals(updatedValue)); // we're not doing this!
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "");
		} catch (Exception e) {
			ex = e;
		}

		if(response.getStatus() != StatusType.DELETE_SUCCESS){
			System.out.println("DELETE RESP INTERACTIONTEST: "+ response.getStatus() + "\n\n\n========\n\n\n");
		}
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);

	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

//	@Test
//	public void testGetUnsetValue() {
//		String key = "an unset value";
//		KVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.get(key);
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
//	}
//	


}
