package common.comms;

import common.messages.KVMessage;
import common.messages.Message;

import java.io.*;
import java.util.Map;

public interface ICommListener {
	public void OnKVMsgRcd(KVMessage msg, OutputStream client);
	public void OnTuplesReceived(Map.Entry<?, ?>[] tuples);
	public void OnTuplesRequest(Byte[] lower, Byte[] upper, OutputStream client);
}