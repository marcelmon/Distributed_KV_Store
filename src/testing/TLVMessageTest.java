package testing;

import java.util.Arrays;

import org.junit.Test;
import common.messages.*;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;

import java.io.*;

public class TLVMessageTest extends TestCase {
	@Test
	public void testGetRecovery() {
		TLVMessage msg = new TLVMessage(KVMessage.StatusType.GET, "a", null);
		TLVMessage recoveredMsg = new TLVMessage(msg.getBytes());
		assertTrue(msg.equals(recoveredMsg));
	}
	
	@Test
	public void testGetMarshal() {
		TLVMessage msg = new TLVMessage(KVMessage.StatusType.GET, "ab", null);
		byte[] bMsg = msg.getBytes();
		byte[] truth = {0, 2, 97, 98};
		
		assertTrue(Arrays.equals(bMsg,  truth));
	}
	
	@Test
	public void testGetUnmarshal() {
		byte[] bMsg = {0, 2, 97, 98};		
		TLVMessage msg = new TLVMessage(bMsg);
		TLVMessage truth = new TLVMessage(StatusType.GET, "ab", null); 
		assertTrue(msg.equals(truth));
	}
	
	@Test
	public void testGetMalformed() {
		boolean caught = false;
		try {
			new TLVMessage(KVMessage.StatusType.GET, "a", "b");
		} catch (RuntimeException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
	@Test
	public void testPutRecovery() {
		TLVMessage msg = new TLVMessage(KVMessage.StatusType.PUT, "a", "b");
		TLVMessage recoveredMsg = new TLVMessage(msg.getBytes());
		assertTrue(msg.equals(recoveredMsg));
	}
	
	@Test
	public void testPutMalformed() {
		boolean caught = false;
		try {
			new TLVMessage(KVMessage.StatusType.PUT, "a", null);
		} catch (RuntimeException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
	@Test
	public void testFromStream() {
		TLVMessage truth = new TLVMessage(StatusType.PUT, "b", "c");
		InputStream stream = new ByteArrayInputStream(truth.getBytes());
		try {
			TLVMessage msg = new TLVMessage(stream);
			assertTrue(msg.equals(truth));
		} catch (KVMessage.StreamTimeoutException e) {
			fail("Truncated stream");
		}
	}
	
	@Test
	public void testFromTruncatedStream() {
		TLVMessage truth = new TLVMessage(StatusType.PUT, "b", "c");
		byte[] bytes = truth.getBytes();
		byte[] truncated = new byte[bytes.length-1];
		System.arraycopy(bytes, 0, truncated, 0, bytes.length-1);
		InputStream stream = new ByteArrayInputStream(truncated);
		boolean caught = false;
		try {
			new TLVMessage(stream);
		} catch (KVMessage.StreamTimeoutException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
	@Test
	public void testFromEmptyStream() {
		InputStream stream = new ByteArrayInputStream(new byte[0]);
		boolean caught = false;
		try {
			new TLVMessage(stream);
		} catch (KVMessage.StreamTimeoutException e) {
			caught = true;
		}
		assertTrue(caught);
	}
	
//	@Test
//	public void testFromCorruptStream() {
//		
//	}
}
