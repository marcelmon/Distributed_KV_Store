package common.comms;

import common.messages.*;

public interface ICommMod {
	// Server
	public void StartServer(int port) throws Exception;
	public void StopServer() throws Exception;
	public void SetListener(ICommListener listener);
	
	// Client
	public void Connect(String ip, int port) throws Exception;
	public void Disconnect();	
	public void SendMessage(KVMessage msg) throws Exception;
}
