package common.comms;

import common.comms.IConsistentHasher.ServerRecord;
import common.messages.*;
import java.io.*;
import java.net.UnknownHostException;
import java.util.Map;

public interface ICommMod {
	// Server
	public void StartServer(int port) throws Exception;
	public void StopServer() throws Exception;
	public void SetListener(ICommListener listener);
	public void SendMessage(KVMessage msg, OutputStream client) throws Exception;
	public void SendTuples(Map.Entry<String, String> tuples, OutputStream client) throws Exception;
	public int GetPort();
	
	// Client
	public void Connect(String ip, int port) throws UnknownHostException, Exception;
	public void Disconnect();
	public boolean isConnected();
	public KVMessage SendMessage(KVMessage msg) throws KVMessage.StreamTimeoutException, Exception;
	public void SendTuples(Map.Entry<String, String> tuples) throws Exception;
	public Map.Entry<String, String> GetTuples(Byte[] lower, Byte[] upper) throws Exception;
}
