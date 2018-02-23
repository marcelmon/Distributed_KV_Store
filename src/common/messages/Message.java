package common.messages;

import java.io.BufferedInputStream;

import common.messages.Message.StatusType;

public abstract class Message {
	/**
	 * Thrown when a stream doesn't make data available quickly enough.
	 */
	public class StreamTimeoutException extends Exception {
		private static final long serialVersionUID = 1L;

		public StreamTimeoutException(String msg) {
			super(msg);
		}
	}
	
	public class FormatException extends Exception {
		private static final long serialVersionUID = 1L;

		public FormatException(String msg) {
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
		DELETE_ERROR, 	/* Delete - request not successful */

		SERVER_STOPPED,         /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
		SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */
		
		BULK_REQUEST,   /* A request for multiple tuples (see TLVBulkMessage) */
		BULK_PACKAGE    /* A collection of tuples being sent (see TLVBulkMessage) */
	}
	
	public static Message getInstance(StatusType type) {
		switch (type) {
			case GET:
			case GET_ERROR:
			case GET_SUCCESS:
			case PUT:
			case PUT_SUCCESS:
			case PUT_UPDATE:
			case PUT_ERROR:
			case DELETE_SUCCESS:
			case DELETE_ERROR:
			case SERVER_STOPPED:
			case SERVER_WRITE_LOCK:
			case SERVER_NOT_RESPONSIBLE:
				return new KVMessage();
			case BULK_REQUEST:
				return new BulkRequestMessage();
			case BULK_PACKAGE:
				return new BulkPackageMessage();
			default:
				throw new RuntimeException("Unknown StatusType."); // fatal programmatic error
		}
	}
	
	public abstract boolean equals(Message msg);
	
	public abstract StatusType getStatus();

	/**
	 * @return Converts the KVMessage to a byte array encoding (i.e. marshals).
	 */
	public abstract byte[] getBytes();
	
	/**
	 * Populates the KVMessage from the byte array.
	 * @param bytes 
	 */
	public abstract void fromBytes(byte[] bytes);
	
	/**
	 * Populates the KVMessage from the InputStream.
	 * - Assumes the message begins at the first byte available
	 * - Guaranteed to leave the stream: 
	 *   - if successful, at the first byte after the message (first byte of subsequent message)
	 *   - if StreamTimeoutException, at the initial state of the stream
	 * @param stream
	 * @throws StreamTimeoutException
	 */
	public abstract void fromInputStream(BufferedInputStream stream) throws StreamTimeoutException;
}
