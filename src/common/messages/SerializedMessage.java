package common.messages;

import java.io.Serializable;

public class SerializedMessage implements KVMessage, Serializable {
	//nb: members we don't want to serialize and pass over the wire should
	//be marked 'transient'
	
	StatusType status;
	String key;
	String value;
	
	public SerializedMessage(byte[] bytes) {
		fromBytes(bytes);
	}
	
	public SerializedMessage(StatusType status, String key, String value) {
		this.status = status;
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes() {
		// TODO implement - serialize
		return null;
	}

	@Override
	public void fromBytes(byte[] bytes) {
		// TODO implement - deserialize
		
	}

}
