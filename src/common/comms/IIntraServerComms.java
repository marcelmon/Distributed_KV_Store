package common.comms;

import java.util.List;
import java.util.Map;

import app_kvServer.IKVServer;

public interface IIntraServerComms {
	public enum RPCMethod {
		Start,
		Stop,
		LockWrite,
		UnlockWrite,
		MoveData
	}
	
	/**
	 * Sets the name of the server calling the object. Allows other methods to reference the
	 * name of this server. Should be called during construction if the object is to be used
	 * by an IKVServer (but does not need to be called by ECS).
	 * @param me
	 */
	public void setSelf(String me);
	
	/**
	 * Performs a remote procedure call of the method defined by "type" on the KVServer identified by target.
	 */
	public boolean call(String target, RPCMethod method, String... args);
	
	/**
	 * Populates an IConsistentHasher implementation with the current state according to Zookeeper.
	 * @param hasher (output param)
	 */
	public void getMetadata(IConsistentHasher hasher);
	
	/**
	 * Registers the given IKVServer as receiving remote procedure calls directed to the indicated target.
	 */
	public void register(String target, IKVServer server);
	
	/**
	 * Requests that the designated server enter a write lock - during which it only allows reads.
	 * This request immediately precedes a requestTuples() call and is used to ensure consistency.
	 * Note that the server is liable to release the write lock itself if the subsequent request isn't
	 * sent quickly enough.
	 * @param server
	 */
	public void requestWriteLock(String server);
	
	/**
	 * Requests that the designated server enter leave a write lock. This request immediately follows 
	 * a requestTuples() call and is used to ensure data availability. There is no check of whether
	 * the lock is acquired as the server is able to release the lock itself when it determines a
	 * timeout has occurred.
	 * @param server
	 */
	public void releaseWriteLock(String server);
	
	/**
	 * Receives all tuples from the txServer falling into the hash range [lower, upper). 
	 * @param lower
	 * @param upper
	 * @return
	 */
	public List<Map.Entry<String, String>> requestTuples(String txServer, String lower, String upper);
	
	/**
	 * Sends a list of tuples to the rxServer. Called just prior to a server leaving the system. It is
	 * the responsibility of the callee to ensure that rxServer is correctly chosen as the server which
	 * will takeover responsibility for these tuples when it leaves.
	 * @param rxServer
	 * @param tuples
	 */
	public void sendTuples(String rxServer, List<Map.Entry<String, String>> tuples);
	
	/**
	 * Adds self to Zookeeper. Subsequently, this server will begin receving client requests for its
	 * share of keys. A call to addServer() should occur only after a call to IConsistentHasher.preAddServer()
	 * to receive a server and range of keys, a call to requestWriteLock(), a call to requestTuples(), then
	 * a call to releaseWriteLock().
	 */
	public void addServer();
	
	/**
	 * Removes self from Zookeeper. Subsequently, this server will stop receving client requests for its
	 * (previous) share of keys. A call to removeServer() should occur only after a call to 
	 * IConcistentHasher.preRemoveServer() to receive a server, moving oneself into a read only state, 
	 * and then a call to sendTuples().
	 */
	public void removeServer();
}
