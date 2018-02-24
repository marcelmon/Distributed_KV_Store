package common.comms;

import java.util.List;
import java.util.Map;

import app_kvServer.IKVServer;
import common.comms.IIntraServerComms.NotYetRegisteredException;
import common.comms.IIntraServerComms.ServerExistsException;

public interface IIntraServerComms {
	public class ServerExistsException extends Exception {
		private static final long serialVersionUID = 1L;
		public ServerExistsException(String msg) {
			super(msg);
		}
	}
	
	public class NotYetRegisteredException extends Exception {
		private static final long serialVersionUID = 1L;
		public NotYetRegisteredException(String msg) {
			super(msg);
		}
	}
	
	public class InvalidArgsException extends Exception {
		private static final long serialVersionUID = 1L;
		public InvalidArgsException(String msg) {
			super(msg);
		}
	}
	
	public enum RPCMethod {
		Start,
		Stop,
		LockWrite,
		UnlockWrite,
		MoveData
	}
	
	public void init(String hostname, Integer port) throws ServerExistsException;
	
	public void close() throws Exception;
	
	/**
	 * Performs a remote procedure call of the method defined by "type" on the KVServer identified by target.
	 */
	public void call(String target, RPCMethod method, String... args) throws InvalidArgsException, Exception;
	
	/**
	 * Registers the given server as receiving remote procedure calls directed to it. It will only begin
	 * receiving these after adding itself with a call to addServer().
	 */
	public void register(IIntraServerCommsListener server);
	
	/**
	 * Adds self to Zookeeper. Subsequently, this server will begin receving client requests for its
	 * share of keys. A call to addServer() should occur only after a call to IConsistentHasher.preAddServer()
	 * to receive a server and range of keys, a call to requestWriteLock(), a call to requestTuples(), then
	 * a call to releaseWriteLock().
	 */
	public void addServer() throws ServerExistsException, Exception;
	
	/**
	 * Removes self from Zookeeper. Subsequently, this server will stop receving client requests for its
	 * (previous) share of keys. A call to removeServer() should occur only after a call to 
	 * IConcistentHasher.preRemoveServer() to receive a server, moving oneself into a read only state, 
	 * and then a call to sendTuples().
	 */
	public void removeServer() throws NotYetRegisteredException, Exception;
}
