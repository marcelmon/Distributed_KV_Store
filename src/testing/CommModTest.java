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
import java.io.*;

public class CommModTest extends TestCase {
	protected ArrayList<Byte> serverRx = new ArrayList<>();
	protected ArrayList<Byte> clientRx = new ArrayList<>();
	protected TLVMessage serverRx_msg;
	protected TLVMessage clientRx_msg;
	
	private class TestCommListener implements ICommListener {
		protected KVMessage mostRecentMsg;
		protected ICommMod server;
		
		public TestCommListener(ICommMod server) {
			this.server = server;
		}
		
		public KVMessage getMostRecentMsg() {
			return mostRecentMsg;
		}
		
		@Override
		public void OnMsgRcd(KVMessage msg, OutputStream client) {
			mostRecentMsg = msg;
			try {
				server.SendMessage(new TLVMessage(StatusType.PUT, "abc", "def"), client);
			} catch (Exception e) {
				// do nothing - the error will come out in the wash
			}
		}
		
	}
	
	@Test
	public void testToServer() {
		try {
			int port = 12346;
			
			// Generate server and listener:
			CommMod server = new CommMod();
			TestCommListener listener = new TestCommListener(server);
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
	
	@Test
	public void testBidirectional() {
		try {
			int port = 12345;
			
			// Generate server and listener:
			CommMod server = new CommMod();
			TestCommListener listener = new TestCommListener(server);
			server.StartServer(port);
			server.SetListener(listener);
			
			// Create test message:
			KVMessage msg = new TLVMessage(StatusType.PUT, "ab", "cd");
			
			// Generate client:
			CommMod client = new CommMod();
			client.Connect("localhost", port);
			KVMessage resp = client.SendMessage(msg);
			
			// Query we have the correct response:
			assertTrue(new TLVMessage(StatusType.PUT, "abc", "def").equals(resp));
		} catch (Exception e) {
			fail("Exception: " + e.getMessage());
		}
	}
}