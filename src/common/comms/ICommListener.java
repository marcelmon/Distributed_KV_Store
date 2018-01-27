package common.comms;

import common.messages.KVMessage;
import java.io.*;

public interface ICommListener {
	public void OnMsgRcd(KVMessage msg, OutputStream client);
}