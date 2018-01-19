package common.comms;

import common.messages.KVMessage;

public interface ICommListener {
	public void OnMsgRcd(KVMessage msg);
}