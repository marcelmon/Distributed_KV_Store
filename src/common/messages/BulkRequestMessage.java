package common.messages;

import java.io.*;
import java.util.Arrays;

import common.messages.*;
import common.messages.Message.StatusType;
import ecs.IECSNode;

public class BulkRequestMessage extends Message {
	Byte[] lower;
	Byte[] upper;
	final long timeout = 250;
	
	/**
	 * Default constructor.
	 */
	public BulkRequestMessage() { }
	
	/**
	 * Constructs the TLVMessage from it's byte encoding.
	 * @param bytes
	 * @see fromBytes
	 */
	public BulkRequestMessage(byte[] bytes) {
		fromBytes(bytes);
	}
	
	/**
	 * Constructs the TLVMessage from an input stream.
	 * @param bytes
	 * @see fromInputStream
	 */
	public BulkRequestMessage(BufferedInputStream stream) throws StreamTimeoutException {
		fromInputStream(stream);
	}
	
	/**
	 * Constructs the TLVMessage from raw contents.
	 * @param status
	 * @param key
	 * @param value
	 */
	public BulkRequestMessage(Byte[] lower, Byte[] upper) throws FormatException {
		this.lower = lower;
		this.upper = upper;
	}

	@Override
	public StatusType getStatus() {
		return StatusType.BULK_REQUEST;
	}
	
	public Byte[] getLower() {
		return lower.clone();
	}
	
	public Byte[] getUpper() {
		return upper.clone();
	}
	
	protected byte[] toTLV(byte tag, Byte[] lower, Byte[] upper) {
		int len = 1 /*TAG*/ + 2 /*hash lengths*/ + lower.length + upper.length;
		if (len >= 255) {
			throw new RuntimeException("Hashes too long!"); //very unexpected
		}
		if (upper.length >= 255) {
			throw new RuntimeException("Hash too long!"); //very unexpected
		}
		if (lower.length >= 255) {
			throw new RuntimeException("Hash too long"); // very unexpected
		}
		byte[] output = new byte[len];
		output[0] = tag;
	    output[1] = (byte) lower.length;
	    output[2] = (byte) upper.length;
	    byte[] rawlower = new byte[lower.length];
	    for (int i = 0; i < lower.length; i++) rawlower[i] = lower[i];
	    byte[] rawupper = new byte[upper.length];
	    for (int i = 0; i < upper.length; i++) rawupper[i] = upper[i];
		System.arraycopy(
				rawlower,  //srcbuffer 
				0, 				//start pos in src buffer
				output, 		//dst buffer
				1+2,    //start post in dst buffer
				lower.length);   //write len
		System.arraycopy(
				rawupper,  			//src buffer
				0,							//start pos in src buffer 
				output, 					//dst buffer
				1+2+lower.length,    //start pos in dst buffer
				upper.length);				//write len
		return output;
	}
	
	protected void fromTLV(byte[] buffer) {
		byte tag = buffer[0];		
		int lowerlen = buffer[1];
		int upperlen = buffer[2];
		
		if (tag != (byte) StatusType.BULK_REQUEST.ordinal()) {
			throw new RuntimeException("Invalid usage"); // fatally incorrect usage
		}
		
		byte[] lower = new byte[lowerlen];
		System.arraycopy(buffer, 1+2, lower, 0, lowerlen);
		byte[] upper = new byte[upperlen];
		System.arraycopy(buffer, 1+2+lowerlen, upper, 0, upperlen);
		
		this.lower = new Byte[lower.length];
		for (int i=0; i < lower.length; i++)
			this.lower[i] = lower[i];
		this.upper = new Byte[upper.length];
		for (int i=0; i < upper.length; i++)
			this.upper[i] = upper[i];
	}

	@Override
	public byte[] getBytes() {
		byte[] output = null;
		byte tag = (byte) StatusType.BULK_REQUEST.ordinal();
		output = toTLV(tag, lower, upper);
		return output;
	}

	@Override
	public void fromBytes(byte[] bytes) {
		//TODO check for length >= 1
		fromTLV(bytes);
	}
	
	@Override
	public boolean equals(Message msg) {
		if (!StatusType.BULK_REQUEST.equals(msg.getStatus())) {
			return false;
		}
		if (!(msg instanceof BulkRequestMessage)) {
			return false;
		}
		BulkRequestMessage castMsg = (BulkRequestMessage) msg;
		if (!Arrays.equals(castMsg.lower, lower)) {
			return false;
		}
		if (!Arrays.equals(castMsg.upper, upper)) {
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
			if (tag != (byte) StatusType.BULK_REQUEST.ordinal()) {
				throw new RuntimeException("Instantiating a BulkRequstMessage from an invalid stream.");
			}
			int len = stream.available() + 1;
			
			// Calculate remaining length:
			StatusType status = StatusType.values()[tag];
			int msgLen = -1;

			// Allow TIMEOUT ms for data to become available:
			t0 = System.currentTimeMillis();
			while (stream.available() < 1) {
				if (System.currentTimeMillis() - t0 > timeout) {
					stream.reset();
					throw new StreamTimeoutException("Timed out waiting for first byte to appear");
				}
				try {
					Thread.sleep(SLEEPDT);
				} catch ( InterruptedException e) { }
			}
			
			// Read data:
			int lowerlen = stream.read() & 0xFF; // to get unsigned value
			if (lowerlen < 0) {
				throw new RuntimeException("Unexpected negative number");
			}
			int upperlen = stream.read() & 0xFF; // to get unsigned value
			if (upperlen < 0) {
				throw new RuntimeException("Unexpected negative number");
			}
			header = new byte[3];
			header[0] = (byte) tag;
			header[1] = (byte) lowerlen;
			header[2] = (byte) upperlen;
			msgLen = lowerlen + upperlen;
			if (msgLen < 0) {
				throw new RuntimeException("Negative message length"); // programmatic error
			}
			
			// Read remaining message into buffer:
			byte[] buffer = new byte[header.length + msgLen];
			System.arraycopy(header, 0, buffer, 0, header.length);
			
			// Allow TIMEOUT ms for data to become available:
			t0 = System.currentTimeMillis();
			while (stream.available() < msgLen) {
				if (System.currentTimeMillis() - t0 > timeout) {
					stream.reset();
					throw new StreamTimeoutException("Timed out waiting for first byte to appear");
				}
				try {
					Thread.sleep(SLEEPDT);
				} catch ( InterruptedException e) { }
			}
			
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
