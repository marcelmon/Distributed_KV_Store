package client;

import common.comms.*;
import common.messages.*;
import common.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private ICommMod commMode = null;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		commMode = new CommMod();
	}

	@Override
	public void connect() throws Exception {
		commMode.Connect(this.address,this.port);
	}

	@Override
	public void disconnect() {
		commMode.Disconnect();
		commMode = null;
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		StatusType statusType = StatusType.PUT;
		KVMessage kvmsg = new TLVMessage(statusType,key,value);
		commMode.SendMessage(kvmsg);
		return kvmsg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		StatusType statusType = StatusType.GET;
		KVMessage kvmsg = new TLVMessage(statusType,key,null);
		commMode.SendMessage(kvmsg);
		return kvmsg;
	}
}
