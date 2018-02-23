package testing;

import org.junit.Test;

import common.messages.BulkRequestMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.Message.StatusType;
import junit.framework.TestCase;

public class MessageTest extends TestCase {
	@Test
	public void testKVMessage() {
		byte[] buffer = new byte[] {
			(byte) StatusType.GET.ordinal(),
			1,
			'a'
		};
		Message msg = Message.getInstance(StatusType.GET);
		msg.fromBytes(buffer);
		assertTrue(msg instanceof KVMessage);
		KVMessage cast = (KVMessage) msg;
		assertTrue(cast.getKey().equals("a"));
		assertTrue(cast.getValue() == null);
	}
	
	@Test
	public void testBulkRequestMessage() {
		//TODO
//		byte[] buffer = new byte[] {
//			(byte) StatusType.BULK_REQUEST.ordinal(),
//			2,
//			2,
//			'a',
//			'b',
//			'c',
//			'd'
//		};
//		Byte[] lower = new Byte[] {'a', 'b'};
//		Byte[] upper = new Byte[] {'c', 'd'};
//		Message msg = Message.getInstance(StatusType.BULK_REQUEST);
//		msg.fromBytes(buffer);
//		assertTrue(msg instanceof BulkRequestMessage);
//		BulkRequestMessage cast = (BulkRequestMessage) msg;
//		assertTrue(cast.getLower().equals(lower));
//		assertTrue(cast.getUpper().equals(upper));
	}
	
	@Test
	public void testBulkPackageMessage() {
		// TODO
	}
}
