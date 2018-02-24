package testing;

import java.io.BufferedInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Test;

import common.messages.BulkPackageMessage;
import common.messages.BulkRequestMessage;
import common.messages.Message.StatusType;
import junit.framework.TestCase;

public class BulkMessageTest extends TestCase {
	@Test
	public void testBulkRequestInit() throws Exception {
		Byte[] lower = new Byte[] {'a', 'b'};
		Byte[] upper = new Byte[] {'c', 'd'};
		
		BulkRequestMessage msg = new BulkRequestMessage(lower, upper);
		
		assertTrue(Arrays.equals(msg.getLower(), lower));
		assertTrue(Arrays.equals(msg.getUpper(), upper));
	}
	
	@Test
	public void testBulkRequestBytes() throws Exception {
		byte[] buffer = new byte[] {
			(byte) StatusType.BULK_REQUEST.ordinal(),
			2,
			3,
			'a',
			'b',
			'c',
			'd',
			'e'
		};
		
		Byte[] lower = new Byte[] {'a', 'b'};
		Byte[] upper = new Byte[] {'c', 'd', 'e'};
		
		BulkRequestMessage msg = new BulkRequestMessage();
		msg.fromBytes(buffer);
		
		assertTrue(Arrays.equals(msg.getLower(), lower));
		assertTrue(Arrays.equals(msg.getUpper(), upper));
	}
	
	@Test
	public void testBulkRequestStream() throws Exception {
		byte[] buffer = new byte[] {
			(byte) StatusType.BULK_REQUEST.ordinal(),
			2,
			3,
			'a',
			'b',
			'c',
			'd',
			'e'
		};
		
		// Stream:
		PipedOutputStream out = new PipedOutputStream();
		PipedInputStream in = new PipedInputStream(out);
		BufferedInputStream stream = new BufferedInputStream(in);
		out.write(buffer, 0, buffer.length);
		
		Byte[] lower = new Byte[] {'a', 'b'};
		Byte[] upper = new Byte[] {'c', 'd', 'e'};
		
		BulkRequestMessage msg = new BulkRequestMessage();
		msg.fromInputStream(stream);
		
		assertTrue(Arrays.equals(msg.getLower(), lower));
		assertTrue(Arrays.equals(msg.getUpper(), upper));
	}
	
	@Test
	public void testBulkPackageInit() throws Exception {
		final int N = 4;
		Map.Entry<?, ?>[] tuples = new AbstractMap.SimpleEntry<?, ?>[4];
		for (int i = 0; i < N; i++)
			tuples[i] = new AbstractMap.SimpleEntry<String, String>(
					Integer.toString(i), 
					Integer.toString(i+10));
		BulkPackageMessage msg = new BulkPackageMessage(tuples);
		
		Map.Entry<?, ?>[] rxTuples = msg.getTuples();
		assertTrue(Arrays.deepEquals(tuples, rxTuples));
	}
	
	@Test
	public void testBulkPackageBytes0() throws Exception {		
		byte[] buffer = new byte[] {
			(byte) StatusType.BULK_PACKAGE.ordinal(),
			0, 0, 0, 8, // 4 bytes of length
			1,
			1,
			'a',
			'b',
			1,
			1,
			'c',
			'd'
		};
		
		BulkPackageMessage msg = new BulkPackageMessage();
		msg.fromBytes(buffer);
		
		Map.Entry<?, ?>[] expected = new AbstractMap.SimpleEntry<?, ?>[] {
			new AbstractMap.SimpleEntry<String, String>("a", "b"),
			new AbstractMap.SimpleEntry<String, String>("c", "d"),
		};		
		
		Map.Entry<?, ?>[] received = msg.getTuples();
		
		assertTrue(Arrays.deepEquals(received, expected));
	}
	
	@Test
	public void testBulkPackageBytes1() throws Exception {
		byte[] buffer = new byte[] {
			(byte) StatusType.BULK_PACKAGE.ordinal(),
			0, 0, 0, 20, // 4 bytes of length
			4,
			5,
			't', 'e', 's', 't',
			'v', 'a', 'l', 'u', 'e',
			5,
			2,
			't', 'e', 's', 't', '2',
			'd', 'v'
		};
		
		BulkPackageMessage msg = new BulkPackageMessage();
		msg.fromBytes(buffer);
		
		Map.Entry<?, ?>[] expected = new AbstractMap.SimpleEntry<?, ?>[] {
			new AbstractMap.SimpleEntry<String, String>("test", "value"),
			new AbstractMap.SimpleEntry<String, String>("test2", "dv"),
		};
		
		Map.Entry<?, ?>[] received = msg.getTuples();
		
		assertTrue(Arrays.deepEquals(received, expected));
	}
	
	@Test
	public void testBulkPackageStream() throws Exception {
		byte[] buffer = new byte[] {
			(byte) StatusType.BULK_PACKAGE.ordinal(),
			0, 0, 0, 20, // 4 bytes of length
			4,
			5,
			't', 'e', 's', 't',
			'v', 'a', 'l', 'u', 'e',
			5,
			2,
			't', 'e', 's', 't', '2',
			'd', 'v'
		};
		
		// Stream:
		PipedOutputStream out = new PipedOutputStream();
		PipedInputStream in = new PipedInputStream(out);
		BufferedInputStream stream = new BufferedInputStream(in);
		out.write(buffer, 0, buffer.length);
		
		BulkPackageMessage msg = new BulkPackageMessage();
		msg.fromInputStream(stream);
		
		Map.Entry<?, ?>[] expected = new AbstractMap.SimpleEntry<?, ?>[] {
			new AbstractMap.SimpleEntry<String, String>("test", "value"),
			new AbstractMap.SimpleEntry<String, String>("test2", "dv"),
		};
		
		Map.Entry<?, ?>[] received = msg.getTuples();
		
		assertTrue(Arrays.deepEquals(received, expected));
	}
	
	@Test
	public void testLargeMessage() throws Exception {
		// final int N = (int) Math.pow(10, 1); // 10^6 elements
		final int N = (int) Math.pow(10, 3);
		final int klen = 255;
		final int vlen = 255;
		AbstractMap.SimpleEntry<?, ?>[] tuples = new AbstractMap.SimpleEntry<?,?>[N];
		for (int i = 0; i < N; i++) {
			byte[] rawk = new byte[klen];
			byte[] rawv = new byte[vlen];
//			new Random().nextBytes(rawk);
//			new Random().nextBytes(rawv);
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
			
//			System.out.println("k[" + i + "]=" + k);
//			System.out.println("v[" + i + "]=" + v);
			
			tuples[i] = new AbstractMap.SimpleEntry<String, String>(k, v);
		}
		
		BulkPackageMessage tx_msg = new BulkPackageMessage(tuples);
		BulkPackageMessage rx_msg = new BulkPackageMessage(tx_msg.getBytes());
		
//		for (byte b : tx_msg.getBytes()) {
//			System.out.println("b:" + (b & 0xff));
//		}
		
		Entry<?, ?>[] tx = tx_msg.getTuples();
		Entry<?, ?>[] rx = rx_msg.getTuples();
		
//		System.out.println("txlen=" + tx.length);
//		System.out.println("rxlen=" + rx.length);
		assertTrue(tx.length == rx.length);
		for (int i = 0; i < tx.length; i++) {
			String rx_key = (String) rx[i].getKey();
			String tx_key = (String) tx[i].getKey();
			String rx_value = (String) rx[i].getValue();
			String tx_value = (String) tx[i].getValue();
			
//			System.out.println("rx:" + rx_key + ">" + rx_value);
//			System.out.println("tx:" + tx_key + ">" + tx_value);
			
			assertTrue(rx_key.equals(tx_key));
			assertTrue(rx_value.equals(tx_value));
		}
	}
}
