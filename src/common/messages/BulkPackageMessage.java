package common.messages;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class BulkPackageMessage extends Message {
	// For Java reasons, we have to use the unbounded wildcard here so we can form an array of
	// a generic type. Not very clean, but we must know everywhere that the key/value types
	// are string or else an error can arise!
	Map.Entry<?, ?> tuples[] = null;
	final long timeout = 3000;
	final int sizeofInt = 4;
	
	/**
	 * Default constructor.
	 */
	public BulkPackageMessage() { }
	
	/**
	 * Constructs the BulkPackageMessage from it's byte encoding.
	 * @param bytes
	 * @see fromBytes
	 */
	public BulkPackageMessage(byte[] bytes) {
		fromBytes(bytes);
	}
	
	/**
	 * Constructs the BulkPackageMessage from an input stream.
	 * @param bytes
	 * @see fromInputStream
	 */
	public BulkPackageMessage(BufferedInputStream stream) throws StreamTimeoutException {
		fromInputStream(stream);
	}
	
	/**
	 * Constructs the BulkPackageMessage from raw contents.
	 */
	public BulkPackageMessage(Map.Entry<?, ?>[] tuples) throws FormatException {
		fromTuples(tuples);
	}

	@Override
	public StatusType getStatus() {
		return StatusType.BULK_PACKAGE;
	}
	
	public Map.Entry<?, ?>[] getTuples() {
		return tuples;
	}
	
	/**
	 * 
	 * @param tuples of type Map.Entry<String, String>
	 * @return
	 */
	protected byte[] toTLV(Map.Entry<?, ?>[] tuples) {
		int len = 0;
		for (Map.Entry<?, ?> t : tuples) {
			String key = (String) t.getKey();
			String value = (String) t.getValue();
			int keylen = key.length();
			int valuelen = value.length();
			if (keylen >= 255) {
				// TODO make non fatal
				throw new RuntimeException("Key is too long!");
			}
			if (valuelen >= 255) {
				// TODO make non fatal
				throw new RuntimeException("Value is too long!");
			}
			len += 2 + keylen + valuelen; // 2 is for key and value lengths  
		}
		
		if (len >= 255) {
			//TODO make non fatal
			throw new RuntimeException("Bulk package too long!");
		}		
		
		byte[] output = new byte[1+4+len];
		output[0] = (byte) StatusType.BULK_PACKAGE.ordinal();
		ByteBuffer bb = ByteBuffer.allocate(sizeofInt).putInt(len);
		System.arraycopy(
				bb.array(), 
				0,
				output,
				1,
				sizeofInt);
		
		int cursor = 1+sizeofInt;
		for (Map.Entry<?, ?> t : tuples) {
			String key = (String) t.getKey();
			String value = (String) t.getValue();
			int kl = key.length();
			int vl = value.length();
			output[cursor] = (byte) kl;
			output[cursor+1] = (byte) vl;
			
			// write key
			System.arraycopy(
					key.getBytes(),  //src buffer 
					0, 				//start pos in src buffer
					output, 		//dst buffer
					cursor+2,    //start post in dst buffer
					kl);   //write len
			
			// write value:
			System.arraycopy(
					value.getBytes(),  //src buffer 
					0, 				//start pos in src buffer
					output, 		//dst buffer
					cursor+2+kl,    //start post in dst buffer
					vl);   //write len
			
			cursor += 2 + kl + vl;
		}
		
		for (byte b : output) {
			System.out.println("o:" + b);
		}
		
		return output;
	}
	
	protected void fromTLV(byte[] buffer) {
		for (byte b : buffer) {
			System.out.println("b:" + b);
		}
		
		byte tag = buffer[0];
		byte[] rawlen = new byte[sizeofInt];
		System.arraycopy(
				buffer, 
				1,
				rawlen,
				0,
				sizeofInt);
		ByteBuffer bb = ByteBuffer.allocate(sizeofInt).wrap(rawlen);
		// bb.order(ByteOrder.BIG_ENDIAN);
		int len = bb.getInt(0);	
		
		ArrayList<AbstractMap.SimpleEntry<String, String>> lTuples = 
				new ArrayList<AbstractMap.SimpleEntry<String, String>>();
		
		int cursor = 1+sizeofInt;
		while (cursor < len+1+sizeofInt) { 
			System.out.println(cursor + "/" + len);
			int kl = buffer[cursor];
			int vl = buffer[cursor+1];
			
			System.out.println("kl=" + kl);
			System.out.println("vl=" + vl);
			
			// Read key
			byte[] rawkey = new byte[kl];
			System.arraycopy(
					buffer,  //src buffer 
					cursor+2, 				//start pos in src buffer
					rawkey, 		//dst buffer
					0,    //start post in dst buffer
					kl);   //write len
			String key = new String(rawkey);
			
			// Read value
			byte[] rawvalue = new byte[vl];
			System.arraycopy(
					buffer,  //src buffer 
					cursor+2+kl, 				//start pos in src buffer
					rawvalue, 		//dst buffer
					0,    //start post in dst buffer
					vl);   //write len
			String value = new String(rawvalue);
			
//			System.out.println(key + ":" + value);
			
			// Add to list:
			lTuples.add(new AbstractMap.SimpleEntry<String, String>(key, value));
			
			cursor += 2 + kl + vl;
		}
		
		tuples = lTuples.toArray(new AbstractMap.SimpleEntry<?, ?>[0]);
	}

	@Override
	public byte[] getBytes() {
		return toTLV(tuples);
	}

	@Override
	public void fromBytes(byte[] bytes) {
		//TODO check for length >= 1
		fromTLV(bytes);
	}
	
	public void fromTuples(Map.Entry<?, ?>[] tuples) {
		this.tuples = tuples;
	}
	
	@Override
	public boolean equals(Message msg) {
		if (!StatusType.BULK_PACKAGE.equals(msg.getStatus())) {
			return false;
		}
		if (!(msg instanceof BulkPackageMessage)) {
			return false;
		}
		BulkPackageMessage castMsg = (BulkPackageMessage) msg;
		if (!Arrays.deepEquals(castMsg.tuples, tuples)) {
			return false;
		}
		return true;
	}

	@Override
	public void fromInputStream(BufferedInputStream stream) throws StreamTimeoutException {
		// If we have a timeout, we should reset the stream to the initial state
		if (!stream.markSupported() ) {
			throw new RuntimeException("Marks not supported in streams. There is a risk of data loss.");
		}
		stream.mark(1024);
		
		try {
			byte[] header;
			
			final long SLEEPDT = 10;
	
			// Read tag:
			long t0 = System.currentTimeMillis();
			while (stream.available() < 1) {
				if (System.currentTimeMillis() - t0 > timeout) {
					stream.reset();
					throw new StreamTimeoutException("Timed out waiting for first byte to appear");
				}
				try {
					Thread.sleep(SLEEPDT);
				} catch ( InterruptedException e) { }
			}
			int tag = stream.read() & 0xFF; // to get unsigned value
			if (tag != (byte) StatusType.BULK_PACKAGE.ordinal()) {
				throw new RuntimeException("Instantiating a BulkPackageMessage from an invalid stream.");
			}
			
			// Allow TIMEOUT ms for length to become available:
			t0 = System.currentTimeMillis();
			while (stream.available() < sizeofInt) {
				if (System.currentTimeMillis() - t0 > timeout) {
					stream.reset();
					throw new StreamTimeoutException("Timed out waiting for second byte to appear");
				}
				try {
					Thread.sleep(SLEEPDT);
				} catch ( InterruptedException e) { }
			}
			
			header = new byte[1+sizeofInt];
			header[0] = (byte) tag;
			
			byte[] rawlen = new byte[sizeofInt];
			stream.read(rawlen, 0, sizeofInt);
			int msgLen = ByteBuffer.allocate(sizeofInt).wrap(rawlen).getInt();
			if (msgLen < 0) {
				throw new RuntimeException("Negative message length"); // programmatic error
			}
			System.arraycopy(rawlen, 0, header, 1, sizeofInt);

			// Allow TIMEOUT ms for data to become available:
			t0 = System.currentTimeMillis();
			while (stream.available() < msgLen) {
				if (System.currentTimeMillis() - t0 > timeout) {
					stream.reset();
					throw new StreamTimeoutException("Timed out waiting for body to appear");
				}
				try {
					Thread.sleep(SLEEPDT);
				} catch ( InterruptedException e) { }
			}
			
			// Read remaining message into buffer:
			byte[] buffer = new byte[header.length + msgLen];
			System.arraycopy(header, 0, buffer, 0, header.length);
			
			// Read data:
			stream.read(buffer, header.length, msgLen);
			if (buffer.length != msgLen + header.length) {
				throw new RuntimeException("Unexpected error occurred!");
			}
			
			// Construct from the bytes:
			fromBytes(buffer);
		} catch (IOException e) {
			//TODO handle
		}
	}
}
