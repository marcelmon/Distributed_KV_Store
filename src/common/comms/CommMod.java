package common.comms;

import common.messages.KVMessage;

public class CommMod implements ICommMod {
	String tx_ip;
	Integer tx_port;
	Integer rx_port;
	ICommListener listener;
	
	public CommMod(String tx_ip, Integer tx_port, Integer rx_port) {
		this.tx_ip = tx_ip;
		this.tx_port = tx_port;
		this.rx_port = rx_port;
		
		//TODO spawn thread and wait for incoming connections (forwarding to listener)
	}
	
	@Override
	public void SetListener(ICommListener listener) {
		this.listener = listener;
	}

	@Override
	public boolean SendMessage(KVMessage msg) {
		//TODO connect to server
		//TODO serialize msg
		//TODO transmit msg
		//TODO disconnect
		
		return false; //TODO report status
	}

}
