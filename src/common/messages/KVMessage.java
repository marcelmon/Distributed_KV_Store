package common.messages;

import java.io.InputStream;

public interface KVMessage {
	/**
	 * Thrown when a stream doesn't make data available quickly enough.
	 */
	public class StreamTimeoutException extends Exception {
		private static final long serialVersionUID = 1L;

		public StreamTimeoutException(String msg) {
			super(msg);
		}
	}
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR 	/* Delete - request successful */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
	/**
	 * @return Converts the KVMessage to a byte array encoding (i.e. marshals).
	 */
	public byte[] getBytes();
	
	/**
	 * Populates the KVMessage from the byte array.
	 * @param bytes 
	 */
	public void fromBytes(byte[] bytes);
	
	/**
	 * Populates the KVMessage from the inputstream.
	 * - Assumes the message begins at the first byte available
	 * - Guaranteed to leave the stream at the first byte after the message
	 * @param stream
	 * @throws StreamTimeoutException
	 */
	public void fromInputStream(InputStream stream) throws StreamTimeoutException;
	
	/**
	 * Returns true if this KVMessage is equal to another.
	 */
	public boolean equals(KVMessage msg);
}


