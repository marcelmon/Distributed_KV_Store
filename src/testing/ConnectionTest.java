package testing;

import java.net.UnknownHostException;

import app_kvServer.KVServer;
import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	protected KVServer server;

	@Override
	public void setUp() throws Exception {
		server = new KVServer("localhost", 50000, "localhost", 2181, 10, "LFU"); // TODO put proper args when zookeeper implemented
		server.run();
	}
	
	@Override
	public void tearDown() {
		server.close();
		server = null;
	}
	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.get("");
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.get("");
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.get("");
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}	
}

