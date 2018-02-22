package client;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import common.comms.*;
import common.comms.IConsistentHasher.*;
import common.messages.*;
import common.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {
	private IConsistentHasher hasher;
	private ICommMod client = null;
	
	/**
	 * Initialize KVStore with address and port of (initial) KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		client = new CommMod();
		
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord(address, port));
		
		hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
	}

	@Override
	public void disconnect() {
		if (client.isConnected()) {
			client.Disconnect();
		}
	}

	@Override
	public boolean isConnected() {
		return client.isConnected(); 
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO call client.connect on appropriate server (if not already connected to approriate server)
		// TODO handle case whereby server responds that it is not correct (i.e. update metadata and retry)
		// TODO fail after a certain number of failures 
		
		String ip = hasher.getServerList()[0].hostname;
		Integer port = hasher.getServerList()[0].port;
		client.Connect(ip, port);
		
		StatusType statusType = StatusType.PUT;
		KVMessage tx_msg = new TLVMessage(statusType,key,value);
		KVMessage rx_msg = null;
		try {
			rx_msg = client.SendMessage(tx_msg);
		} catch (KVMessage.StreamTimeoutException e) {
			//TODO log error
		}
		return rx_msg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO call client.connect on appropriate server (if not already connected to approriate server)
		// TODO handle case whereby server responds that it is not correct (i.e. update metadata and retry)
		// TODO fail after a certain number of failures
		
		String ip = hasher.getServerList()[0].hostname;
		Integer port = hasher.getServerList()[0].port;
		client.Connect(ip, port);
		
		StatusType statusType = StatusType.GET;
		KVMessage tx_msg = new TLVMessage(statusType,key,null);
		KVMessage rx_msg = null;
		try {
			rx_msg = client.SendMessage(tx_msg);
		} catch (KVMessage.StreamTimeoutException e) {
			System.out.println("Stream timeout");
			//TODO log error
		}
		return rx_msg;
	}
}
