package testing;

import java.util.Arrays;

import org.junit.Test;
import common.messages.*;
import common.messages.Message.StatusType;
import junit.framework.TestCase;

import java.io.*;

public class KVMessageTest extends TestCase {
	@Test
	public void testGetRecovery() {
		try {
			KVMessage msg = new KVMessage(KVMessage.StatusType.GET, "a", null);
			KVMessage recoveredMsg = new KVMessage(msg.getBytes());
			assertTrue(msg.equals(recoveredMsg));
		} catch (KVMessage.FormatException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetMarshal() {
		try {
			KVMessage msg = new KVMessage(KVMessage.StatusType.GET, "ab", null);
			byte[] bMsg = msg.getBytes();
			byte[] truth = {0, 2, 97, 98};
			
			assertTrue(Arrays.equals(bMsg,  truth));
		} catch (KVMessage.FormatException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetUnmarshal() {
		try {
			byte[] bMsg = {0, 2, 97, 98};		
			KVMessage msg = new KVMessage(bMsg);
			KVMessage truth = new KVMessage(StatusType.GET, "ab", null); 
			assertTrue(msg.equals(truth));
		} catch (KVMessage.FormatException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetMalformed() {
		boolean caught = false;
		try {
			new KVMessage(KVMessage.StatusType.GET, "a", "b");
		} catch (KVMessage.FormatException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
	@Test
	public void testPutRecovery() {
		try {
			KVMessage msg = new KVMessage(KVMessage.StatusType.PUT, "a", "b");
			KVMessage recoveredMsg = new KVMessage(msg.getBytes());
			assertTrue(msg.equals(recoveredMsg));
		} catch (KVMessage.FormatException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testPutMalformed() {
		boolean caught = false;
		try {
			new KVMessage(KVMessage.StatusType.PUT, "a", null);
		} catch (KVMessage.FormatException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
	@Test
	public void testFromStream() {
		try {
			KVMessage truth = new KVMessage(StatusType.PUT, "b", "c");
			InputStream stream = new ByteArrayInputStream(truth.getBytes());
			BufferedInputStream bufStream = new BufferedInputStream(stream);
			try {
				KVMessage msg = new KVMessage(bufStream);
				assertTrue(msg.equals(truth));
			} catch (KVMessage.StreamTimeoutException e) {
				fail("Truncated stream");
			}
		} catch (KVMessage.FormatException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFromTruncatedStream() {
		try {
			KVMessage truth = new KVMessage(StatusType.PUT, "b", "c");
			byte[] bytes = truth.getBytes();
			byte[] truncated = new byte[bytes.length-1];
			System.arraycopy(bytes, 0, truncated, 0, bytes.length-1);
			InputStream stream = new ByteArrayInputStream(truncated);
			BufferedInputStream bufStream = new BufferedInputStream(stream);
			boolean caught = false;
			try {
				new KVMessage(bufStream);
			} catch (KVMessage.StreamTimeoutException e) {
				caught = true;
			}
			assertTrue(caught);
		} catch (KVMessage.FormatException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFromEmptyStream() {
		InputStream stream = new ByteArrayInputStream(new byte[0]);
		BufferedInputStream bufStream = new BufferedInputStream(stream);
		boolean caught = false;
		try {
			new KVMessage(bufStream);
		} catch (KVMessage.StreamTimeoutException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
	@Test
	public void testRecoveryFromStreamTimeout() {
		try {
			// Correct message:
			KVMessage truth = new KVMessage(StatusType.PUT, "b", "c");
			final byte[] bytes = truth.getBytes();
			
			// Stream:
			PipedOutputStream out = new PipedOutputStream();
			PipedInputStream in = new PipedInputStream(out);
			BufferedInputStream stream = new BufferedInputStream(in);
			
			//Transmit first part:
			assertTrue(bytes.length >= 3);
			out.write(bytes, 0, 2); // first two bytes
			boolean caught = false;
			try {
				new KVMessage(stream);
			} catch (KVMessage.StreamTimeoutException e) {
				caught = true;
			}
			assertTrue(caught);
			
			//Transmit second part:
			out.write(bytes, 2, bytes.length-2);  // the rest
			KVMessage rx;
			try {
				rx = new KVMessage(stream);
				
				// Check for correctness:
				assertTrue(rx != null);
				assertTrue(rx.equals(truth));
			} catch (KVMessage.StreamTimeoutException e) {
				fail();
			}
			
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetSuccess() {
		try {
			KVMessage msg = new KVMessage(StatusType.GET_SUCCESS, "a", "b");
			KVMessage recovered = new KVMessage(msg.getBytes());
			assertTrue(msg.equals(recovered));
		} catch (KVMessage.FormatException e) {
			fail("Unexpected format exception");
		}
	}
	
	@Test
	public void testUnevenLengths() {
		try {
			KVMessage msg = new KVMessage(StatusType.PUT, "abc", "defghi");
			KVMessage recovered = new KVMessage(msg.getBytes());
			assertTrue(msg.equals(recovered));
		} catch (KVMessage.FormatException e) {
			fail("Unexpected format exception");
		}
		
		try {
			KVMessage msg = new KVMessage(StatusType.PUT, "abcdef", "bcd");
			KVMessage recovered = new KVMessage(msg.getBytes());
			assertTrue(msg.equals(recovered));
		} catch (KVMessage.FormatException e) {
			fail("Unexpected format exception");
		}
	}
	
//	@Test
//	public void testFromCorruptStream() {
//		
//	}
}
