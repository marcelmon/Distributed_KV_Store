package client;

import java.net.UnknownHostException;

import common.comms.*;
import common.messages.*;
import common.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private ICommMod client = null;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		client = new CommMod();
	}

	@Override
	public void connect() throws UnknownHostException, Exception {
		client.Connect(address, port);
	}

	@Override
	public void disconnect() {
		client.Disconnect();
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
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
