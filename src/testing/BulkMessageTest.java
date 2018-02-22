package testing;

import java.io.BufferedInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

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
	public void testBulkPackageBytes() throws Exception {
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
}
