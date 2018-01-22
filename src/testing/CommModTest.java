package testing;

import org.junit.Test;
import common.messages.*;
import common.messages.KVMessage.*;
import common.comms.*;
import junit.framework.TestCase;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.lang.Thread;

public class CommModTest extends TestCase {
	protected ArrayList<Byte> serverRx = new ArrayList<>();
	protected ArrayList<Byte> clientRx = new ArrayList<>();
	protected TLVMessage serverRx_msg;
	protected TLVMessage clientRx_msg;
	
	private class TestCommListener implements ICommListener {
		protected KVMessage mostRecentMsg;
		
		public KVMessage getMostRecentMsg() {
			return mostRecentMsg;
		}
		
		@Override
		public void OnMsgRcd(KVMessage msg) {
			mostRecentMsg = msg;
		}
		
	}
	
	@Test
	public void testFullBidirectional() {
		try {
			int port = 12345;
			
			// Create a listener object for the server:
			TestCommListener listener = new TestCommListener();
			
			// Generate server:
			CommMod server = new CommMod();
			server.StartServer(port);
			server.SetListener(listener);
			
			// Create test message:
			KVMessage msg = new TLVMessage(StatusType.PUT, "ab", "cd");
			
			// Generate client:
			CommMod client = new CommMod();
			client.Connect("localhost", port);
			client.SendMessage(msg);
			
			// Give server time to process:
			Thread.sleep(250);
			
			// Query that the listener has the correct message:
			assertTrue(listener.getMostRecentMsg() != null);
			assertTrue(listener.getMostRecentMsg().equals(msg));
		} catch (Exception e) {
			fail("Exception: " + e.getMessage());
		}
	}
}