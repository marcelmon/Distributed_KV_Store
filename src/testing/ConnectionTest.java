package testing;

import java.net.UnknownHostException;

import app_kvServer.KVServer;
import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	protected KVServer server;

	@Override
	public void setUp() throws Exception {
		server = new KVServer(50000, 10, "LFU");
		server.run();
	}
	
	@Override
	public void tearDown() {
		server.close();
	}
	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}	
}

