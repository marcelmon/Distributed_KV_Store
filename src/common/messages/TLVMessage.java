package common.messages;

import java.io.*;

public class TLVMessage implements KVMessage {
	String key;
	String value;
	StatusType status;
	
	/**
	 * Constructs the TLVMessage from it's byte encoding.
	 * @param bytes
	 * @see fromBytes
	 */
	public TLVMessage(byte[] bytes) {
		fromBytes(bytes);
	}
	
	/**
	 * Constructs the TLVMessage from an input stream.
	 * @param bytes
	 * @see fromInputStream
	 */
	public TLVMessage(InputStream stream) {
		fromInputStream(stream);
	}
	
	/**
	 * Constructs the TLVMessage from raw contents.
	 * @param status
	 * @param key
	 * @param value
	 */
	public TLVMessage(StatusType status, String key, String value) {		
		this.key = key;
		this.value = value;
		this.status = status;
		
		// Format checks:
		if (status != StatusType.PUT && value != null) {
			throw new RuntimeException("Value on non-PUT TLVMessage");
			
//			System.out.println("WARNING! Dropping value on non-PUT TLVMessage");
//			this.value = null;
		}
		if (status == StatusType.PUT && value == null) {
			throw new RuntimeException("No value on PUT TLVMessage");
			
//			System.out.println("WARNING! null->empty value conversion on PUT TLVMessage");
//			this.value = "";
		}
	}
	
	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public StatusType getStatus() {
		return status;
	}
	
	protected byte[] toTLV1Field(byte tag, String f0) {
		int numFields = 1;
		int len = 1 /*TAG*/ + numFields /*LENGTHS*/ + f0.length();
		byte[] output = new byte[len];
		output[0] = tag;
	    output[1] = (byte)f0.length();
	    System.arraycopy(
				f0.getBytes(),  //src buffer 
				0, 				//start pos in src buffer
				output, 		//dst buffer
				1+numFields,    //start post in dst buffer
				f0.length());   //write len
	    return output;
	}
	
	protected byte[] toTLV2Field(byte tag, String f0, String f1) {
		int numFields = 2;
		int len = 1 /*TAG*/ + numFields /*LENGTHS*/ + f0.length() + f1.length();
		if (len >= 255) {
			//TODO throw exception
		}
		byte[] output = new byte[len];
		output[0] = tag;
	    output[1] = (byte)f0.length();
	    output[2] = (byte)f1.length();
		System.arraycopy(
				f0.getBytes(),  //src buffer 
				0, 				//start pos in src buffer
				output, 		//dst buffer
				1+numFields,    //start post in dst buffer
				f0.length());   //write len
		System.arraycopy(
				f1.getBytes(),  			//src buffer
				0,							//start pos in src buffer 
				output, 					//dst buffer
				1+numFields+f1.length(),    //start pos in dst buffer
				f1.length());				//write len
		return output;
	}
	
	protected void fromTLV1Field(StatusType status, byte[] buffer) {
		// Sanity checks:
		if (status != this.status) {
			//TODO throw exception
			//This is just a sanity check to ensure the formats are correct
		}
		if (status == StatusType.PUT) {
			//TODO throw exception - not 1 field
		}
		
		int keyLen = buffer[1];
		byte[] keyBytes = new byte[keyLen];
		System.arraycopy(buffer, 2, keyBytes, 0, keyLen);
		key = new String(keyBytes);
		
		value = null;
	}
	
	protected void fromTLV2Field(StatusType status, byte[] buffer) {
		// Sanity checks:
		if (status != this.status) {
			//TODO throw exception
			//This is just a sanity check to ensure the formats are correct
		}
		if (status != StatusType.PUT) {
			//TODO throw exception - not 2 field
		}
		
		int keyLen = buffer[1];
		int valueLen = buffer[2];
		
		byte[] keyBytes = new byte[keyLen];
		System.arraycopy(buffer, 3, keyBytes, 0, keyLen);
		byte[] valueBytes = new byte[valueLen];
		System.arraycopy(buffer, 3+keyLen, valueBytes, 0, valueLen);
		
		key = new String(keyBytes);
		value = new String(valueBytes);
	}

	@Override
	public byte[] getBytes() {
		byte[] output = null;
		byte tag = (byte) status.ordinal();
		
		if (status == StatusType.PUT) {
			output = toTLV2Field(tag, key, value);
		} else {
			output = toTLV1Field(tag, key);
		}				
		
		return output;
	}

	@Override
	public void fromBytes(byte[] bytes) {
		//TODO check for length >= 1
		
		status = StatusType.values()[bytes[0]];
		
		if (status == StatusType.PUT) {
			fromTLV2Field(status, bytes);
		} else {
			fromTLV1Field(status, bytes);
		}
	}
	
	@Override
	public boolean equals(KVMessage msg) {
		boolean eqStatus = status.equals(msg.getStatus());
		boolean eqKey = key.equals(msg.getKey());
		boolean eqValue = (value == null && msg.getValue() == null) || value.equals(msg.getValue());
		
//		System.out.println(eqStatus);
//		System.out.println(eqKey);
//		System.out.println(eqValue);
		
		return eqStatus && eqKey && eqValue;
	}

	@Override
	public void fromInputStream(InputStream stream) {
		try {
			byte[] header;
	
			// Read tag:
			int tag = stream.read();
			if (tag < 0) {
				//TODO handle error
			}
			int len = stream.available() + 1;
			
			// Calculate remaining length:
			System.out.println("tag:" + tag);
			StatusType status = StatusType.values()[tag];
			int msgLen = -1;
			if (status == StatusType.PUT) {
				if (len < 3) {
					//TODO not enough for tag + 2 fields' lengths => error
				}
				int l0 = stream.read();
				int l1 = stream.read();
				if (l0 < 0 || l1 < 0) {
					//TODO handle error
				}
				msgLen = l0 + l1;
				header = new byte[3];
				header[0] = (byte) tag;
				header[1] = (byte) l0;
				header[2] = (byte) l1;
			} else {
				if (len < 2) {
					//TODO not enough for tag + 1 field's lengths => error
				}
				int l0 = stream.read();
				if (l0 < 0) {
					//TODO handle error
				}
				msgLen = l0;
				header = new byte[2];
				header[0] = (byte) tag;
				header[1] = (byte) l0;
			}
			
			// Read remaining message into buffer:
			byte[] buffer = new byte[header.length + msgLen];
			System.arraycopy(header, 0, buffer, 0, header.length);
			stream.read(buffer, header.length, msgLen);
			
			// Construct from the bytes:
			fromBytes(buffer);
		} catch (IOException e) {
			//TODO handle
		}
	}
}
