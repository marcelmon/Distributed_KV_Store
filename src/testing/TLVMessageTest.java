package testing;

import java.util.Arrays;

import org.junit.Test;
import common.messages.*;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import junit.runner.Version;

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
		TLVMessage truth = new TLVMessage(StatusType.GET, "a", null);
		InputStream stream = new ByteArrayInputStream(truth.getBytes());
		TLVMessage msg = new TLVMessage(stream);
		assertTrue(msg.equals(truth));
	}
}
