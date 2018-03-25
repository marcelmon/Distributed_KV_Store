package testing;

import org.junit.Test;

import common.comms.CommMod;
import common.messages.KVMessage;
import common.messages.Message.StatusType;
import junit.framework.TestCase;
import app_kvServer.*;
import app_kvServer.ICache.KeyDoesntExistException;

public class KVServerTests extends TestCase {
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
}
