package common.comms;

import common.messages.KVMessage;
import java.io.*;
import java.util.Map;

public interface ICommListener {
	public void OnMsgRcd(KVMessage msg, OutputStream client);
	public void OnTuplesReceived(Map.Entry<String, String>[] tuples);
	public Map.Entry<String, String>[] OnTuplesRequest(Byte[] lower, Byte[] upper);
}