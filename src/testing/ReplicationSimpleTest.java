package testing;


import org.junit.*;

import app_kvServer.KVServer;

import app_kvServer.ICache;


import client.KVStore;

import junit.framework.TestCase;


import common.messages.Message.StatusType;

import common.messages.*;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


import java.io.*;

import java.net.BindException;




import java.util.Arrays;

import java.util.List;

import java.util.*;


import common.comms.ConsistentHasher;

import common.comms.IConsistentHasher;
import common.comms.IConsistentHasher.ServerRecord;
import common.comms.IIntraServerComms.RPCMethod;
import common.comms.IIntraServerCommsListener;
import common.comms.IntraServerComms;
import common.comms.IntraServerComms.RPCRecord;
import junit.framework.TestCase;



import java.util.ArrayList;


import java.util.Arrays;


	


import java.util.HashMap;



import java.security.MessageDigest;


import java.security.NoSuchAlgorithmException;


import java.util.*;

public class ReplicationSimpleTest extends TestCase implements Watcher {


	public static Byte[] subtractOne(Byte[] A) {

		Byte[] newArr = new Byte[A.length];
		System.arraycopy(A, 0, newArr, 0, A.length);
	    int i = newArr.length - 1;  // start at the last position

	    // Looping from right to left
		while(i >= 0 && newArr[i] == 0 ){
			newArr[i] = (byte) (255 & 0xFF);
			i--;
		}
		if(i < 0){ // have looped around (all were 0 and will now be 255)
			return newArr;
		}
		byte theOne = (byte) (1);
		byte theA = (byte) (newArr[i]%256);
		byte subOne = (byte) ((theA - theOne)%256);
		Byte byteSubOne = new Byte(subOne);
		newArr[i] = byteSubOne;
		return newArr;
	}


	public static Byte[] addOne(Byte[] A) {

		Byte[] newArr = new Byte[A.length];
		System.arraycopy(A, 0, newArr, 0, A.length);

	    int i = newArr.length - 1;  // start at the last position
	    // Looping from right to left
		while( i >= 0 && (newArr[i] & 0xFF) == 255 ){
			if((newArr[i] & 0xFF) == 255){
			}
			newArr[i] = 0;
			i--;
		}
		if(i < 0){ // have looped around (all were 255 and will now be all 0s)
			return newArr;
		}
		byte theOne = (byte) (1);
		byte theA = (byte) (newArr[i]%256);
		byte plusOne = (byte) ((theA + theOne)%256);
		Byte bytePlusOne = new Byte(plusOne);

		newArr[i] = bytePlusOne;
		return newArr;
	}




	// protected KVServer server1 = null;
	// protected KVServer server2 = null;
	// protected KVServer server3 = null;


	private final String zkAddr = "127.0.0.1:2181";
	private final String clusterGroup = "/cluster";
	private final String rpcGroup = "/rpc";

	private final String thisHost = "localhost";


	// @Override
	// public void tearDown() {
	// 	if(server1 != null){
	// 		server1.close();
	// 		server1 = null;
	// 	}
	// 	if(server2 != null){
	// 		server2.close();
	// 		server2 = null;
	// 	}
	// 	if(server3 != null){
	// 		server3.close();
	// 		server3 = null;
	// 	}
	// }


	@Override
	public void process(WatchedEvent event) {
		// do nothing	
	}


	@Override
	public void setUp() throws Exception {

		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		
		// Create the cluster group if it doesn't exist:
		try {
			zk.create(clusterGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (NodeExistsException e) {
			// don't care
		}
		
		// Clear it out:
		List<String> servernodes = zk.getChildren(clusterGroup, false);
		for (String n : servernodes) {
			try {
				Stat stat = zk.exists(clusterGroup + "/" + n, false);
				zk.delete(clusterGroup + "/" + n, stat.getVersion());
			} catch (KeeperException e) {
				if (e.code() != KeeperException.Code.NONODE) throw e;
			}
		}
		
		// Create the rpc group if it doesn't exist:
		try {
			zk.create(rpcGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (NodeExistsException e) {
			// don't care
		}
		
		// Clear it out:
		List<String> rpcnode = zk.getChildren(rpcGroup, false);
		for (String n : rpcnode) {
			try {
				Stat stat = zk.exists(rpcGroup + "/" + n, false);
				if (stat != null)
					zk.delete(rpcGroup + "/" + n, stat.getVersion());
			} catch (KeeperException e) {
				if (e.code() != KeeperException.Code.NONODE) throw e;
			}
		}
		Thread.sleep(100); // give zookeeper time to execute
	}




	/*
	 * @param keys 
	 * @param values
	 *
	 *
	 * @param keysToHash - return value mapping all keys to their hashed Byte[] value
	 * @param allHashes  - return value of ServerRecord ordering ServerRecord.hash in ascending order
	*/
	public void getOrderedHashValues(String[] keys, String[] values, HashMap<String, Byte[]> keysToHash, ArrayList<ServerRecord> allHashes){

		

		for (int i =0; i < keys.length; ++i) {
			Byte[] hash = new Byte[0];
			try {
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] primHash = md.digest(keys[i].getBytes());
				hash = new Byte[primHash.length];
				for (int j = 0; j < primHash.length; j++) {
					hash[j] = primHash[j];
				}

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage()); // fatal exception
			}	
			keysToHash.put(keys[i], hash);
			allHashes.add(new ServerRecord(hash));
		}
		Collections.sort(allHashes, new IConsistentHasher.HashComparator());
	}


	@Test
	public void testReplicateCanRecover() throws Exception {
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		int replication = 1;

		int port1 = 10100;
		KVServer server1 = new KVServer(thisHost, port1, "localhost", 2181, 10, "FIFO", replication);
		server1.run();
		server1.clearStorage();
		server1.start();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);


		int port2 = 10101;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO", replication);
		server2.run();
		server2.clearStorage();
		server2.start();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 2);
		// add all these key values

		String[] keys = {"a","b","c","d","e","f","g","h","i","j", "k" , "l", "m", "n", "o", "p", "q"};
		String[] values={"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17"};


		KVStore store1 = new KVStore(thisHost, port1);
		for (int i =0; i < keys.length; ++i) {
			store1.put(keys[i], values[i]);
		}

		// check that the data was succesfully added
		for (int i = 0; i < keys.length; ++i ) {
			KVMessage respGet = store1.get(keys[i]);
			assertTrue(respGet.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(respGet.getValue().equals(values[i]));
		}






		int port3 = 10102;
		KVServer server3 = new KVServer(thisHost, port3, "localhost", 2181, 10, "FIFO", replication);
		server3.run();
		server3.clearStorage();
		server3.start();

		Thread.sleep(1000);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 3);


		// check that not all data is on server1 and server2, but the keys that are found on them should have some missing

		ArrayList<String> missingKeysFrom1 = new ArrayList<String>();
		for (int i = 0; i < keys.length; ++i) {
			
			if(!server1.inCache(keys[i])){
				missingKeysFrom1.add(keys[i]);
			}
			else{
				String resp = server1.getKV(keys[i]);
				assertTrue(resp.equals(values[i]));
			}
		}

		assertTrue(missingKeysFrom1.size() > 0); 



		ArrayList<String> missingKeysFrom2 = new ArrayList<String>();
		for (int i = 0; i < keys.length; ++i) {
			
			if(!server2.inCache(keys[i])){
				missingKeysFrom2.add(keys[i]);
			}
			else{
				String resp = server2.getKV(keys[i]);
				assertTrue(resp.equals(values[i]));
			}
		}

		assertTrue(missingKeysFrom2.size() > 0);





		int total_missing = 0;

		ArrayList<String> missingFromBoth = new ArrayList<String>();

		for(String missingIn1 : missingKeysFrom1){

			boolean isInTwo = false;
			for(String missingIn2 : missingKeysFrom2) {
				if(missingIn2.equals(missingIn2)){
					isInTwo = true;
					break;
				}
				if(isInTwo == false){
					missingFromBoth.add(missingIn1);
				}
			}
		}

		assertTrue(missingFromBoth.size() > 0);
	}


}


