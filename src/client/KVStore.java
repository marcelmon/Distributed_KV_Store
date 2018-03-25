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
		if(hasher.getServerList().length < 1){
			throw new Exception("Error in get. No servers in hasher.");
		}
		
		int numTries = 0;
		int maxTries = 5;
		
		KVMessage rx_msg = null;
		
		try {
	
			client.Disconnect();
			while (true) {
				numTries++;
				if (numTries > maxTries) {
					throw new Exception("Failed to find a server");
				}
				
				ServerRecord toServ = hasher.mapKey(key);
				if (toServ == null) throw new Exception("No servers");
				client.Connect(toServ.hostname, toServ.port);
				
				if (!client.isConnected()) {
					// This server has failed, so remove it and try again:
					ArrayList<ServerRecord> servList = new ArrayList<ServerRecord>();
					for (ServerRecord rec : hasher.getServerList()) servList.add(rec);
					if (servList.isEmpty()) throw new Exception("No servers");
					servList.remove(toServ);
					hasher.fromServerList(servList);
					
					continue; // retry
				}
				numTries = 0;
			
				KVMessage tx_msg = new KVMessage(StatusType.PUT,key,value);
				
				rx_msg = client.SendMessage(tx_msg);
				
				if(rx_msg.getStatus().equals(StatusType.SERVER_WRITE_LOCK)){
					Thread.sleep(250); // wait it out
					if (numTries == maxTries) {
						throw new Exception("Timed out waiting for a locked server");
					}
					continue; // retry
				} else if(rx_msg.getStatus().equals(StatusType.SERVER_STOPPED)){
					// This server has stopped, so remove it and try again:
					ArrayList<ServerRecord> servList = new ArrayList<ServerRecord>();
					for (ServerRecord rec : hasher.getServerList()) servList.add(rec);
					if (servList.isEmpty()) throw new Exception("No servers");
					servList.remove(toServ);
					hasher.fromServerList(servList);
					
					continue; // retry
				} else if(rx_msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)){
					// This server isn't responsible, so we don't have an up-to-date hash
					// ring.
					
					// Update our hash ring with the response:
					hasher.fromString(rx_msg.getKey());
					
					continue; // retry
				} else {
					break;
				}
			}	
		} catch (KVMessage.StreamTimeoutException e) {
			System.out.println("Stream timeout");
			//TODO log error
		}
		return rx_msg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if(hasher.getServerList().length < 1){
			throw new Exception("Error in get. No servers in hasher.");
		}
		
		int numTries = 0;
		int maxTries = 5;
		
		KVMessage rx_msg = null;
		
		try {
	
			client.Disconnect();
			while (true) {
				numTries++;
				if (numTries > maxTries) {
					throw new Exception("Failed to find a server");
				}
				
				ServerRecord toServ = hasher.mapKey(key);
				client.Connect(toServ.hostname, toServ.port);
				
				if (!client.isConnected()) {
					// This server has failed, so remove it and try again:
					ArrayList<ServerRecord> servList = new ArrayList<ServerRecord>();
					for (ServerRecord rec : hasher.getServerList()) servList.add(rec);
					if (servList.isEmpty()) throw new Exception("No servers");
					servList.remove(toServ);
					hasher.fromServerList(servList);
					
					continue; // retry
				}
				numTries = 0;
				
				StatusType statusType = StatusType.GET;
				KVMessage tx_msg = new KVMessage(statusType,key,null);
				
				rx_msg = client.SendMessage(tx_msg);
				
				if(rx_msg.getStatus().equals(StatusType.SERVER_STOPPED)){
					// This server has stopped, so remove it and try again:
					ArrayList<ServerRecord> servList = new ArrayList<ServerRecord>();
					for (ServerRecord r : hasher.getServerList()) servList.add(r);
					if (servList.isEmpty()) throw new Exception("No servers");
					servList.remove(toServ);
					hasher.fromServerList(servList);
					
					continue; // retry
				} else if(rx_msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)){
					// This server isn't responsible, so we don't have an up-to-date hash
					// ring.
					
					// Update our hash ring with the response:
					hasher.fromString(rx_msg.getKey());
					
					continue; // retry
				} else {
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
