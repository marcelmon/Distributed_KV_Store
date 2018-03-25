package common.comms;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

import common.comms.IConsistentHasher.ServerRecord;

public interface IConsistentHasher {
	/**
	 * Thrown in fromString() if the input metadata is malformed.
	 */
	public class StringFormatException extends Exception {
		private static final long serialVersionUID = 1L;
		public StringFormatException(String msg) {
			super(msg);
		}
	}
	
	public class ServerRecord {
		public String hostname;
		public Integer port;
		public Byte[] hash;
		
		public String toString() {
			return hostname + ":" + port.toString();
		}
		
		public ServerRecord(String hostname, Integer port) {
			this.hostname = hostname;
			this.port = port;
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] primHash = md.digest(toString().getBytes());
				hash = new Byte[primHash.length];
				for (int i = 0; i < primHash.length; i++) {
					hash[i] = primHash[i];
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage()); // fatal exception
			}
		}
		
		public void setEqualTo(ServerRecord in) {
			hostname = in.hostname;
			port = in.port;
			hash = in.hash;
		}
		
		public boolean equals(ServerRecord s) {
			return s.hostname.equals(hostname) && s.port.equals(port);
		}
		
		public ServerRecord(Byte[] hash) {
			this.hash = hash;
		}
	}
	
	public class HashComparator implements Comparator<ServerRecord> {
		public int compare(Byte[] arg0, Byte[] arg1) {
			if (arg0.length < arg1.length) {
				return -1;
			} else if (arg0.length > arg1.length) {
				return 1;
			}
			
			// same length
			for (int i = 0; i < arg0.length; i++) {
				if (arg0[i] < arg1[i]) {
					return -1;
				} else if (arg0[i] > arg1[i]) {
					return 1;
				}
			}
			return 0;
		}
		
		public int compare(byte[] arg0, byte[] arg1) {
			if (arg0.length < arg1.length) {
				return -1;
			} else if (arg0.length > arg1.length) {
				return 1;
			}
			
			// same length
			for (int i = 0; i < arg0.length; i++) {
				if (arg0[i] < arg1[i]) {
					return -1;
				} else if (arg0[i] > arg1[i]) {
					return 1;
				}
			}
			return 0;
		}
		
		public int compare(Byte[] arg0, byte[] arg1) {
			if (arg0.length < arg1.length) {
				return -1;
			} else if (arg0.length > arg1.length) {
				return 1;
			}
			
			// same length
			for (int i = 0; i < arg0.length; i++) {
				if (arg0[i] < arg1[i]) {
					return -1;
				} else if (arg0[i] > arg1[i]) {
					return 1;
				}
			}
			return 0;
		}
		
		public int compare(byte[] arg0, Byte[] arg1) {
			if (arg0.length < arg1.length) {
				return -1;
			} else if (arg0.length > arg1.length) {
				return 1;
			}
			
			// same length
			for (int i = 0; i < arg0.length; i++) {
				if (arg0[i] < arg1[i]) {
					return -1;
				} else if (arg0[i] > arg1[i]) {
					return 1;
				}
			}
			return 0;
		}
		
		public int compare(ServerRecord arg0, ServerRecord arg1) {
			return compare(arg0.hash, arg1.hash);
		}		
	}
	
	// Initialization methods
	
	public void fromString(String metadata) throws StringFormatException;
	
	public String toString();
	
	public void fromServerList(List<ServerRecord> servers);
	
	public void fromServerListString(List<String> servers) throws StringFormatException;
	
	public ServerRecord[] getServerList();
	
	// Normal use methods:
	
	/**
	 * Returns the target which "key" maps to.
	 */
	public ServerRecord mapKey(String key);
	
	/**
	 * Returns an array of servers which the key should be redundantly stored at.
	 * Used by KVServer when a PUT request is serviced to forward to redundant 
	 * Doesn't include the "coordinator" server (i.e. the server indicated by
	 * mapKey())
	 */
	public List<ServerRecord> mapKeyRedundant(String key, int N);
	
	/**
	 * Prior to adding this server to the ring of hashes, we need to find a server to receive our share of
	 * keys from. This method will not cause any changes to be communicated. It is intended that this call is 
	 * followed by communication with the txServer to receive tuples, and then a call to 
	 * IIntraServerComms.addServer() to ensure this server gets future requests for those keys.
	 * @param me The name and port of the server being added.
	 * @param txServer (output param) The name and port of the server to receive tuples from. 
	 * @param newRangeLower (output param) The hash of the lowest key the added server is now responsible for.
	 * @param newRangeUpper (output param) The hash of the *lowest* key (greater than the lower key) the added server is 
	 * *not* responsible for (considering keys wrap circularly).
	 */
	public void preAddServer(ServerRecord me, ServerRecord txServer, List<Byte> newRangeLower, List<Byte> newRangeUpper);
	
	/**
	 * Prior to removing this server from the ring of hashes, we need to find a server to send our share of
	 * keys to. This method will not cause any changes to be communicated. It is intended that this call is 
	 * followed by communication with the rxServer to send tuples, and then a call to 
	 * IIntraServerComms.removeServer() to ensure that server gets future requests for those keys.
	 * @param me The name and port of the server being removed.
	 * @param rxServer (output param) The name and port of the server to send tuples to 
	 */
	public void preRemoveServer(ServerRecord me, ServerRecord rxServer);
}
