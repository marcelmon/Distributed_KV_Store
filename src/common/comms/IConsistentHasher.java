package common.comms;

import java.util.List;

public interface IConsistentHasher {
	// Initialization methods
	
	public void fromString(String metadata);
	
	public String toString();
	
	public void fromServerList(List<String> servers);
	
	// Normal use methods:
	
	/**
	 * Returns the target which "key" maps to.
	 */
	public String mapKey(String key);
	
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
	public void preAddServer(String me, String txServer, String newRangeLower, String newRangeUpper);
	
	/**
	 * Prior to removing this server from the ring of hashes, we need to find a server to send our share of
	 * keys to. This method will not cause any changes to be communicated. It is intended that this call is 
	 * followed by communication with the rxServer to send tuples, and then a call to 
	 * IIntraServerComms.removeServer() to ensure that server gets future requests for those keys.
	 * @param me The name and port of the server being removed.
	 * @param rxServer (output param) The name and port of the server to send tuples to 
	 */
	public void preRemoveServer(String me, String rxServer);
}
