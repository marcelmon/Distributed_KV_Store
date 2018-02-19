package common.comms;

import common.messages.*;
import java.io.*;
import java.net.UnknownHostException;

public interface ICommMod {
	// Server
	public void StartServer(int port) throws Exception;
	public void StopServer() throws Exception;
	public void SetListener(ICommListener listener);
	public void SendMessage(KVMessage msg, OutputStream client) throws Exception;
	public int GetPort();
	
	// Client
	public void Connect(String ip, int port) throws UnknownHostException, Exception;
	public void Disconnect();
	public KVMessage SendMessage(KVMessage msg) throws KVMessage.StreamTimeoutException, Exception;
}
