package testing;

import org.junit.Test;
import common.messages.*;
import common.messages.Message.*;
import common.comms.*;
import junit.framework.TestCase;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.lang.Thread;
import java.io.*;

public class CommModPerfTest extends TestCase {
	private class BulkCommListener implements ICommListener {
		private boolean _received = false;
		Entry<?, ?>[] tuples = null;
		
		public boolean received() {
			return _received;
		}
		
		@Override
		public void OnKVMsgRcd(KVMessage msg, OutputStream client) {}
		
		@Override
		public void OnTuplesReceived(Entry<?, ?>[] tuples) {
			this.tuples = tuples;
			_received = true;
		}
		
		@Override
		public void OnTuplesRequest(Byte[] lower, Byte[] upper, OutputStream client) { }
	}
	
	@Test
	public void testBigBulkSendLargeTuples() throws Exception {
		int port = 1132;		
		
		// Generate server and listener:
		CommMod server = new CommMod();
		server.StartServer(port);
		BulkCommListener listener = new BulkCommListener();
		server.SetListener(listener);
		
		CommMod client = new CommMod();
		client.Connect("localhost", port);
		
		final int N = 1000; // max transmission size is about 0.5MB
		final int klen = 255; // this is the maximum key size
		final int vlen = 255; // this is the maximum value size
		AbstractMap.SimpleEntry<?, ?>[] tuples = new AbstractMap.SimpleEntry<?,?>[N];
		for (int i = 0; i < N; i++) {
			byte[] rawk = new byte[klen];
			byte[] rawv = new byte[vlen];
//					new Random().nextBytes(rawk);
//					new Random().nextBytes(rawv);
			for (int j = 0; j < klen; j++) {
				// rawk[j] = (byte) ('a' + (j % 26));
				rawk[j] = (byte) ('a' + (new Random().nextInt(26)));
			}
			for (int j = 0; j < vlen; j++) {
				// rawv[j] = (byte) ('A' + (j % 26));
				rawv[j] = (byte) ('A' + (new Random().nextInt(26)));
			}
			
			// note that some bytes will be control characters so string length will be lower 
			
			String k = new String(rawk);
			String v = new String(rawv);
			
//					System.out.println("k[" + i + "]=" + k);
//					System.out.println("v[" + i + "]=" + v);
			
			tuples[i] = new AbstractMap.SimpleEntry<String, String>(k, v);
		}		
				
		client.SendTuples(tuples);
		
		long t0 = System.currentTimeMillis();
		while (!listener.received()) {
			assertFalse(System.currentTimeMillis() - t0 > 3000);  // it's taking too long if >3sec
			Thread.sleep(500);
		}
		assertTrue(Arrays.deepEquals(tuples, listener.tuples));
	}
	
	@Test
	public void testBigBulkSendSmallTuples() throws Exception {
		int port = 1134;		
		
		// Generate server and listener:
		CommMod server = new CommMod();
		server.StartServer(port);
		BulkCommListener listener = new BulkCommListener();
		server.SetListener(listener);
		
		CommMod client = new CommMod();
		client.Connect("localhost", port);
		
		final int N = 10000; // max transmission size is about 0.5MB
		final int klen = 25; // this is the maximum key size
		final int vlen = 25; // this is the maximum value size
		AbstractMap.SimpleEntry<?, ?>[] tuples = new AbstractMap.SimpleEntry<?,?>[N];
		for (int i = 0; i < N; i++) {
			byte[] rawk = new byte[klen];
			byte[] rawv = new byte[vlen];
//					new Random().nextBytes(rawk);
//					new Random().nextBytes(rawv);
			for (int j = 0; j < klen; j++) {
				// rawk[j] = (byte) ('a' + (j % 26));
				rawk[j] = (byte) ('a' + (new Random().nextInt(26)));
			}
			for (int j = 0; j < vlen; j++) {
				// rawv[j] = (byte) ('A' + (j % 26));
				rawv[j] = (byte) ('A' + (new Random().nextInt(26)));
			}
			
			// note that some bytes will be control characters so string length will be lower 
			
			String k = new String(rawk);
			String v = new String(rawv);
			
//					System.out.println("k[" + i + "]=" + k);
//					System.out.println("v[" + i + "]=" + v);
			
			tuples[i] = new AbstractMap.SimpleEntry<String, String>(k, v);
		}		
				
		client.SendTuples(tuples);
		
		long t0 = System.currentTimeMillis();
		while (!listener.received()) {
			assertFalse(System.currentTimeMillis() - t0 > 3000);  // it's taking too long if >3sec
			Thread.sleep(500);
		}
		assertTrue(Arrays.deepEquals(tuples, listener.tuples));
	}
}