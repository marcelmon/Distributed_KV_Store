package common.messages;

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
	 * Constructs the TLVMessage from raw contents.
	 * @param status
	 * @param key
	 * @param value
	 */
	public TLVMessage(StatusType status, String key, String value) {
		this.key = key;
		this.value = value;
		this.status = status;
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

	@Override
	public byte[] getBytes() {
		// TODO <tag> <length(s)> <data field(s)>
		return null;
	}

	@Override
	public void fromBytes(byte[] bytes) {
		// TODO Auto-generated method stub
	}
}
