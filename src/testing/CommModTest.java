package testing;

import org.junit.Test;
import common.messages.*;
import common.messages.Message.*;
import common.comms.*;
import junit.framework.TestCase;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.lang.Thread;
import java.io.*;

public class CommModTest extends TestCase {
	protected ArrayList<Byte> serverRx = new ArrayList<>();
	protected ArrayList<Byte> clientRx = new ArrayList<>();
	protected KVMessage serverRx_msg;
	protected KVMessage clientRx_msg;
	
	private class KVCommListener implements ICommListener {
		protected Message mostRecentMsg;
		protected ICommMod server;
		
		public KVCommListener(ICommMod server) {
			this.server = server;
		}
		
		public Message getMostRecentMsg() {
			return mostRecentMsg;
		}
		
		@Override
		public void OnKVMsgRcd(KVMessage msg, OutputStream client) {
			mostRecentMsg = msg;
			try {
				server.SendMessage(new KVMessage(StatusType.PUT, "abc", "def"), client);
			} catch (Exception e) {
				// do nothing - the error will come out in the wash
			}
		}

		@Override
		public void OnTuplesReceived(Entry<?, ?>[] tuples) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void OnTuplesRequest(Byte[] lower, Byte[] upper, OutputStream client) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	private class BulkCommListener implements ICommListener {
		private boolean _received = false;
		private boolean _correct = false;
		
		public boolean received() {
			return _received;
		}
		
		public boolean correct() {
			return _correct;
		}
		
		@Override
		public void OnKVMsgRcd(KVMessage msg, OutputStream client) {}
		
		@Override
		public void OnTuplesReceived(Entry<?, ?>[] tuples) {
			System.out.println("rx");
			boolean c = true;
			if (tuples.length != 3) {
				_correct = false;
				_received = true;
			} else {			
				c &= tuples[0].getKey().equals("key0");
				c &= tuples[0].getValue().equals("value0");
				c &= tuples[1].getKey().equals("key1");
				c &= tuples[1].getValue().equals("value1");
				c &= tuples[2].getKey().equals("key2");
				c &= tuples[2].getValue().equals("value2");
				_correct = c;
				_received = true;
			}
			System.out.println(_correct);
		}
		
		@Override
		public void OnTuplesRequest(Byte[] lower, Byte[] upper, OutputStream client) {
			System.out.println("Listener gets request");
			Byte[] lowerExp = new Byte[] {'a', 'b', 'c'};
			Byte[] upperExp = new Byte[] {'d', 'e', 'f'};
			
			assertTrue(Arrays.equals(lower, lowerExp));
			assertTrue(Arrays.equals(upper, upperExp));
			
			Entry<?, ?>[] result = new AbstractMap.SimpleEntry<?, ?>[3];
			result[0] = new AbstractMap.SimpleEntry<String, String>("x", "l");
			result[1] = new AbstractMap.SimpleEntry<String, String>("z", "m");
			result[2] = new AbstractMap.SimpleEntry<String, String>("y", "n");
			
			try {
				BulkPackageMessage msg = new BulkPackageMessage(result);
				System.out.println("Listener writing response...");
				client.write(msg.getBytes());
//					byte[] sent = msg.getBytes();
//					for (byte b : sent) {
//						System.out.println("DEBUG:" + b);
//					}
				System.out.println("Listener sent response");
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Something bad happened");
			}
		}
	}
	
	@Test
	public void testKVToServer() throws Exception {
		int port = 12346;
		
		// Generate server and listener:
		CommMod server = new CommMod();
		KVCommListener listener = new KVCommListener(server);
		server.StartServer(port);
		server.SetListener(listener);
		
		// Create test message:
		KVMessage msg = new KVMessage(StatusType.PUT, "ab", "cd");
		
		// Generate client:
		CommMod client = new CommMod();
		client.Connect("localhost", port);
		client.SendMessage(msg);
		
		// Give server time to process:
		Thread.sleep(250);
		
		// Query that the listener has the correct message:
		assertTrue(listener.getMostRecentMsg() != null);
		assertTrue(listener.getMostRecentMsg().equals(msg));
	}
	
	@Test
	public void testKVBidirectional() throws Exception {
		int port = 12345;
		
		// Generate server and listener:
		CommMod server = new CommMod();
		KVCommListener listener = new KVCommListener(server);
		server.StartServer(port);
		server.SetListener(listener);
		
		// Create test message:
		KVMessage msg = new KVMessage(StatusType.PUT, "ab", "cd");
		
		// Generate client:
		CommMod client = new CommMod();
		client.Connect("localhost", port);
		KVMessage resp = client.SendMessage(msg);
		
		// Query we have the correct response:
		assertTrue(new KVMessage(StatusType.PUT, "abc", "def").equals(resp));
	}
	
	@Test
	public void testBulkRequest() throws Exception {
		int port = 1235;		
		
		// Generate server and listener:
		CommMod server = new CommMod();
		BulkCommListener listener = new BulkCommListener();
		server.StartServer(port);
		server.SetListener(listener);
		
		CommMod client = new CommMod();
		client.Connect("localhost", port);
		
		Byte[] lower = new Byte[] {'a', 'b', 'c'};
		Byte[] upper = new Byte[] {'d', 'e', 'f'};
		Entry<?, ?>[] result = client.GetTuples(lower, upper);
		assertTrue(result != null);		
		assertTrue(result.length == 3);
		assertTrue(result[0].getKey().equals("x"));
		assertTrue(result[1].getKey().equals("z"));
		assertTrue(result[2].getKey().equals("y"));
	}
	
	@Test
	public void testBulkSend() throws Exception {
		int port = 1232;		
		
		// Generate server and listener:
		CommMod server = new CommMod();
		server.StartServer(port);
		BulkCommListener listener = new BulkCommListener();
		server.SetListener(listener);
		
		CommMod client = new CommMod();
		client.Connect("localhost", port);
		
		AbstractMap.SimpleEntry<?, ?>[] tuples = new AbstractMap.SimpleEntry<?,?>[3];
		tuples[0] = new AbstractMap.SimpleEntry<String, String>("key0", "value0");
		tuples[1] = new AbstractMap.SimpleEntry<String, String>("key1", "value1");
		tuples[2] = new AbstractMap.SimpleEntry<String, String>("key2", "value2");
		client.SendTuples(tuples);
		
		long t0 = System.currentTimeMillis();
		while (!listener.received()) {
			assertFalse(System.currentTimeMillis() - t0 > 1000);  // it's taking too long if >3sec
			Thread.sleep(50);
		}
		assertTrue(listener.correct());
	}
}