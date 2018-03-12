package client;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import common.comms.*;
import common.comms.IConsistentHasher.*;
import common.messages.*;
import common.messages.Message.StatusType;

import java.util.Arrays;

public class KVStore implements KVCommInterface {
	private IConsistentHasher hasher;
	private ICommMod client = null;
	
	/**
	 * Initialize KVStore with address and port of (initial) KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		client = new CommMod();
		
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord(address, port));
		
		hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
	}

	@Override
	public void disconnect() {
		if (client.isConnected()) {
			client.Disconnect();
		}
	}

	@Override
	public boolean isConnected() {
		return client.isConnected(); 
	}

	public boolean testAddServer(ServerRecord newServ){
		boolean toAdd = true;

		ServerRecord[] servArray = hasher.getServerList();

		ArrayList<ServerRecord> serverList = new ArrayList<ServerRecord>(Arrays.asList(servArray));
		for (int i = 0; i< servArray.length; i++ ) {
			
			if(servArray[i].hostname.equals(newServ.hostname) && servArray[i].port == newServ.port){
				toAdd = false;
				break;
			}
		}
		if(toAdd == true){
			serverList.add(newServ);
			hasher.fromServerList(serverList);
		}
		return toAdd;
	}


	public boolean testRemoveServer(ServerRecord delServ){
		boolean wasRemoved = false;

		ServerRecord[] servArray = hasher.getServerList();
		ArrayList<ServerRecord> newServList = new ArrayList<ServerRecord>();
		for (int i = 0; i< servArray.length; i++ ) {
			if(!servArray[i].hostname.equals(delServ.hostname) && servArray[i].port != delServ.port){
				newServList.add(servArray[i]);
			}
			else{
				wasRemoved = true;
			}
		}
		if(wasRemoved == true){
			hasher.fromServerList(newServList);
		}
		return wasRemoved;
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO call client.connect on appropriate server (if not already connected to approriate server)
		// TODO handle case whereby server responds that it is not correct (i.e. update metadata and retry)
		// TODO fail after a certain number of failures 
		
		// String ip = hasher.getServerList()[0].hostname;
		// Integer port = hasher.getServerList()[0].port;

		if(hasher.getServerList().length < 1){
			throw new Exception("Error in put. No servers in hasher.");
		}

		ServerRecord toServ = hasher.mapKey(key);
		client.Connect(toServ.hostname, toServ.port);
		
		StatusType statusType = StatusType.PUT;
		KVMessage tx_msg = new KVMessage(statusType,key,value);
		KVMessage rx_msg = null;
		int numTries = 0;
		int maxTries = 3;
		try {
			while(numTries < maxTries){
				rx_msg = client.SendMessage(tx_msg);
				numTries++;
				if(rx_msg.getStatus().equals(StatusType.SERVER_WRITE_LOCK)){
					Thread.sleep(250);
				}
				else if(rx_msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)){
					String[] splitAddr = rx_msg.getKey().split(":");
					if(splitAddr.length != 2){
						throw new Exception("Serious error put: got SERVER_NOT_RESPONSIBLE but bad addr returned (" + rx_msg.getKey() + ")" );
					}
					String newHost = splitAddr[0];
					int newPort = Integer.valueOf(splitAddr[1]);
					ServerRecord newServ = new ServerRecord(newHost, newPort);
					testAddServer(newServ);
					client.Connect(newHost, newPort);
				}
				else if(rx_msg.getStatus().equals(StatusType.SERVER_STOPPED)){
					if(testRemoveServer(toServ) == true){
						if(hasher.getServerList().length < 1){
							break;
							// throw new Exception("Error in put retry. No servers in hasher.");
						}
						toServ = hasher.mapKey(key); // try again since it was removed
						client.Connect(toServ.hostname, toServ.port);
					}
				}
				else{
					break;
				}
			}
			// if(numTries >= maxTries){
			// 	if(isWriteLocked
			// 	throw new Exception("Serious error : received SERVER_NOT_RESPONSIBLE too many times ("+numTries+")");
			// }
				
		} catch (KVMessage.StreamTimeoutException e) {
			//TODO log error
		}
		return rx_msg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO call client.connect on appropriate server (if not already connected to approriate server)
		// TODO handle case whereby server responds that it is not correct (i.e. update metadata and retry)
		// TODO fail after a certain number of failures
		
		// String ip = hasher.getServerList()[0].hostname;
		// Integer port = hasher.getServerList()[0].port;
		// client.Connect(ip, port);

		if(hasher.getServerList().length < 1){
			throw new Exception("Error in put. No servers in hasher.");
		}

		ServerRecord toServ = hasher.mapKey(key);
		client.Connect(toServ.hostname, toServ.port);

		
		StatusType statusType = StatusType.GET;
		KVMessage tx_msg = new KVMessage(statusType,key,null);
		KVMessage rx_msg = null;

		int numTries = 0;
		int maxTries = 3;

		try {
			while(numTries < maxTries){
				rx_msg = client.SendMessage(tx_msg);
				numTries++;
				if(rx_msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)){
					String[] splitAddr = rx_msg.getKey().split(":");
					if(splitAddr.length != 2){
						throw new Exception("Serious error get: got SERVER_NOT_RESPONSIBLE but bad addr returned (" + rx_msg.getKey() + ")" );
					}
					String newHost = splitAddr[0];
					int newPort = Integer.valueOf(splitAddr[1]);
					ServerRecord newServ = new ServerRecord(newHost, newPort);
					testAddServer(newServ);
					client.Connect(newHost, newPort);
				}
				else if(rx_msg.getStatus().equals(StatusType.SERVER_STOPPED)){
					if(testRemoveServer(toServ) == true){
						if(hasher.getServerList().length < 1){
							break;
							// throw new Exception("Error in get retry. No servers in hasher.");
						}
						toServ = hasher.mapKey(key); // try again since it was removed
						client.Connect(toServ.hostname, toServ.port);
					}
				}
				else{
					break;
				}
			}
				
		} catch (KVMessage.StreamTimeoutException e) {
			System.out.println("Stream timeout");
			//TODO log error
		}
		return rx_msg;
	}
}
