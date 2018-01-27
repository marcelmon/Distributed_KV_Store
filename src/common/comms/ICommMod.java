package common.comms;

import common.messages.*;
import java.io.*;

public interface ICommMod {
	// Server
	public void StartServer(int port) throws Exception;
	public void StopServer() throws Exception;
	public void SetListener(ICommListener listener);
	public void SendMessage(KVMessage msg, OutputStream client) throws Exception;
	
	// Client
	public void Connect(String ip, int port) throws Exception;
	public void Disconnect();
	public KVMessage SendMessage(KVMessage msg) throws KVMessage.StreamTimeoutException, Exception;
}
