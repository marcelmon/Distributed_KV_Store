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

public class KVServerBulkDataTransferAndConsistentHasherUpdateTest extends TestCase implements Watcher {


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
	public void testStartServer() throws Exception{


		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);
		

		int port = 10000;
		KVServer server1 = new KVServer(thisHost, port, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		
		server1.run();
	
		server1.clearStorage();

		assertTrue(zk.getChildren(clusterGroup, false).size() == 0);
		
		server1.start();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);



		int port2 = 10001;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		
		server2.run();
		server2.clearStorage();

		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);
		
		server2.start();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 2);


		server2.stop();

		Thread.sleep(100);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		server2.start();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 2);

		server2.stop();
		server1.stop();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 0);


		server1.close();
		server1 = null;
		server2.close();
		server2 = null;


	}

	public KVServer openServerAndAddData (
		String host, 
		int port, 
		String zkHost, 
		int zkPort,  
		int cacheSize, 
		String cacheType, 
		String[] keys,
		String[] values ) throws Exception {

		// start a zookeeper client for monitoring
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// start the first kvserver, kvstore, and get the comms and hasher

		KVServer server = new KVServer(host, port, zkHost, zkPort, cacheSize, cacheType); // TODO put proper args when zookeeper implemented
		server.run();
		server.clearStorage();

		// check zookeeper has no nodes added to cluster yet
		assertTrue(zk.getChildren(clusterGroup, false).size() == 0);

		// check that a node is added in start
		server.start();

		Thread.sleep(200);

		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		// add data to the first server using a kvstore
		KVStore store = new KVStore(host, port);

		assertTrue(keys.length == values.length);
		for(int i = 0; i < keys.length; i++){
			store.put(keys[i], values[i]);
		}

		// confirm data is added
		for(int i = 0; i < keys.length; i++){
			KVMessage respGet = store.get(keys[i]);
			assertTrue(respGet.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(respGet.getValue().equals(values[i]));
		}
		zk.close();
		zk = null;
		store.disconnect();
		store = null;

		return server;
	}


	@Test
	public void testLockWrite() throws Exception {

		// // start the first kvserver, kvstore, and get the comms and hasher
		int port1 = 10002;
		int cacheSize = 10;
		int zkPort = 2181;
		String zkHost = "localhost";
		String cacheStrategy = "FIFO";

		String[] keys = {"a","b","c","d","e","f","g","h","i","j", "k" };
		String[] values={"1","2","3","4","5","6","7","8","9","10","11"};

		KVServer server1 = openServerAndAddData(
			thisHost, 
			port1, 
			zkHost, 
			zkPort, 
			cacheSize,
			cacheStrategy,
			keys,
			values
			);

		KVStore store1 = new KVStore(thisHost, port1);

		// get write lock and assert that puts are blocked for kvserver1
		server1.lockWrite();
		try {
			KVMessage resp1 = store1.put(keys[0], values[0]);
			assertTrue(resp1.getStatus().equals(StatusType.SERVER_WRITE_LOCK));
		} catch (Exception e){
			System.out.println("Exception " + e.getMessage());
		}

		// confirm write lock does not block gets
		for(int i = 0; i < keys.length; i++){
			KVMessage respGet = store1.get(keys[i]);
			assertTrue(respGet.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(respGet.getValue().equals(values[i]));
		}

		server1.close();
		server1 = null;

		store1.disconnect();
		store1 = null;

	}

	@Test
	public void testMoveAllData() throws Exception {

		// // start a zookeeper client for monitoring
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// // start the first kvserver, kvstore, and get the comms and hasher
		int port1 = 10003;
		int cacheSize = 10;
		int zkPort = 2181;
		String zkHost = "localhost";
		String cacheStrategy = "FIFO";

		String[] keys = {"a","b","c","d","e","f","g","h","i","j", "k" };
		String[] values={"1","2","3","4","5","6","7","8","9","10","11"};


		KVServer server1 = openServerAndAddData(
			thisHost, 
			port1, 
			zkHost, 
			zkPort, 
			cacheSize,
			cacheStrategy,
			keys,
			values
			);

		KVStore store1 = new KVStore(thisHost, port1);

		// KVServer server1 = new KVServer(thisHost, port1, zkHost, zkPort, cacheSize, cacheStrategy); // TODO put proper args when zookeeper implemented
		


		// start the second kvserver, kvstore, and get the comms and hasher
		int port2 = 10004;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		
		server2.run();
		server2.clearStorage();


		// check zookeeper still has 1 node added to cluster
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		KVStore store2 = new KVStore(thisHost, port2);

		// confirm no data is added in server2
		for(int i = 0; i < keys.length; i++){

			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}
			assertTrue(notHasKey);
		}

		// move ALL data from server 1 to server 2
		byte[] upperBytes = new byte[1];
		upperBytes[0] = 1;

		byte[] lowerBytes = new byte[1];
		lowerBytes[0] = 2;

		// send RPCMethod.MoveData to server1, this will initiate the bulk transfer between server1 to server2
		server2.getIntraServerComms().call(
			"localhost:"+(Integer.toString(port1)), 
			RPCMethod.MoveData,  
			Base64.getEncoder().encodeToString(lowerBytes),  
			Base64.getEncoder().encodeToString(upperBytes), 
			thisHost + ":" + port2
			);

		Thread.sleep(200);

		// check that all data is added to the second server
		for (int i =0; i < keys.length; ++i) {

			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}
			assertFalse(notHasKey);
			assertTrue(kvGetResp.equals(values[i]));
		}

		// server1.unlockWrite();

		server1.close();
		server1 = null;

		server2.close();
		server2 = null;

		store1.disconnect();
		store1 = null;

		store2.disconnect();
		store2 = null;
		

	}

	@Test
	public void testMoveArbitraryAmountOfData() throws Exception{

		// // start a zookeeper client for monitoring
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// // start the first kvserver, kvstore, and get the comms and hasher
		int port1 = 10005;
		int cacheSize = 10;
		int zkPort = 2181;
		String zkHost = "localhost";
		String cacheStrategy = "FIFO";

		String[] keys   = {"a","b","c","d","e","f","g","h","i","j", "k" };
		String[] values = {"1","2","3","4","5","6","7","8","9","10","11"};


		KVServer server1 = openServerAndAddData(
			thisHost, 
			port1, 
			zkHost, 
			zkPort, 
			cacheSize,
			cacheStrategy,
			keys,
			values
			);

		KVStore store1 = new KVStore(thisHost, port1);


		// start the second kvserver, kvstore, and get the comms and hasher
		int port2 = 10006;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		

		server2.run();
		server2.clearStorage();


		// check zookeeper still has only 1 node added to cluster
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		KVStore store2 = new KVStore(thisHost, port2);

		// confirm no data is added in server2
		for(int i = 0; i < keys.length; i++){
			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}
			assertTrue(notHasKey);
		}

	
		// now setup to send only first 3 hashes (from hash=0 to 3rd hash val)

		HashMap<String, Byte[]> keysToHash = new HashMap<String,Byte[]>();
		ArrayList<ServerRecord> allHashes = new ArrayList<ServerRecord> ();

		getOrderedHashValues(keys, values, keysToHash, allHashes);
			

		Byte[] minHash = subtractOne(allHashes.get(0).hash);

		Byte[] maxHash = addOne(allHashes.get(2).hash);

		// convert Byte[] to byte[] for ics2.call
		byte[] byteMinHash = new byte[minHash.length];
		for(int i =0; i < minHash.length; ++i)  byteMinHash[i] = minHash[i];

		byte[] byteMaxHash = new byte[maxHash.length];
		for(int i =0; i < maxHash.length; ++i)  byteMaxHash[i] = maxHash[i];


		// for the first 3 hashed values, record them for comparison later in get -> keysMovedToHash()
		HashMap<String, Byte[]> keysMovedToHash = new HashMap<String, Byte[]>();
		for (int i = 0; i < 3; ++i) {
			Byte[] hashVal = allHashes.get(i).hash;
			for (Map.Entry<String, Byte[]> thisVal : keysToHash.entrySet()) {
				if(Arrays.equals(hashVal, thisVal.getValue())){
					keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
				}
			}
		}


		// send RPCMethod.MoveData to server1, this will initiate the bulk transfer between server1 to server2
		server2.getIntraServerComms().call(
			"localhost:"+ (Integer.toString(port1)), 
			RPCMethod.MoveData, 
			Base64.getEncoder().encodeToString(byteMinHash), 
			Base64.getEncoder().encodeToString(byteMaxHash), 
			thisHost + ":" + port2) ;


		Thread.sleep(200);

		// confirm that the expected keys were moved
		for (int i = 0;  i < keys.length; ++i) {

			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}

			if(keysMovedToHash.containsKey(keys[i])){
				assertFalse(notHasKey); // has key
				assertTrue(kvGetResp.equals(values[i]));
			}
			else{
				assertTrue(notHasKey);
			}
		}


		server1.close();
		server1 = null;

		server2.close();
		server2 = null;

		store1.disconnect();
		store1 = null;

		store2.disconnect();
		store2 = null;


	}


	/*
	 * Uses the hasher.preAddServer() method to test moving the expected hashed values
	*/
	@Test
	public void testMoveProperHashedValues() throws Exception{
		// // start a zookeeper client for monitoring
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// // start the first kvserver, kvstore, and get the comms and hasher
		int port1 = 10007;
		int cacheSize = 10;
		int zkPort = 2181;
		String zkHost = "localhost";
		String cacheStrategy = "FIFO";

		String[] keys   = {"a","b","c","d","e","f","g","h","i","j", "k" };
		String[] values = {"1","2","3","4","5","6","7","8","9","10","11"};


		KVServer server1 = openServerAndAddData(
			thisHost, 
			port1, 
			zkHost, 
			zkPort, 
			cacheSize,
			cacheStrategy,
			keys,
			values
			);

		KVStore store1 = new KVStore(thisHost, port1);


		// start the second kvserver, kvstore, and get the comms and hasher
		int port2 = 10008;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		

		server2.run();
		server2.clearStorage();


		// check zookeeper still has only 1 node added to cluster
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		KVStore store2 = new KVStore(thisHost, port2);

		// confirm no data is added in server2
		for(int i = 0; i < keys.length; i++){
			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}
			assertTrue(notHasKey);
		}

		// get the upper and lower hash values to move
		ServerRecord txServer = new ServerRecord(null);
		List<Byte> minHash = new ArrayList<Byte>();
		List<Byte> maxHash = new ArrayList<Byte>();
		server2.getConsistentHasher().preAddServer(new ServerRecord(thisHost, port2), txServer, minHash, maxHash);

		// convert Byte[] to byte[] for ics2.call
		byte[] byteMinHash = new byte[minHash.size()];
		for(int i =0; i < minHash.size(); ++i)  byteMinHash[i] = minHash.get(i);

		byte[] byteMaxHash = new byte[maxHash.size()];
		for(int i =0; i < maxHash.size(); ++i)  byteMaxHash[i] = maxHash.get(i);


		// convert Byte[] to byte[] for ics2.call
		Byte[] minHashByteArray = new Byte[minHash.size()];
		for(int i =0; i < minHash.size(); ++i)  minHashByteArray[i] = minHash.get(i);

		Byte[] maxHashByteArray = new Byte[maxHash.size()];
		for(int i =0; i < maxHash.size(); ++i)  maxHashByteArray[i] = maxHash.get(i);



		IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();

		boolean hashPassesZero = false;
		if(comp.compare(new ServerRecord(maxHashByteArray), new ServerRecord(minHashByteArray)) < 0){ // max is less than min
			hashPassesZero = true;
		}

		// for the first 3 hashed values, record them for comparison later in get -> keysMovedToHash()
		HashMap<String, Byte[]> keysMovedToHash = new HashMap<String, Byte[]>();


		HashMap<String, Byte[]> keysToHash = new HashMap<String,Byte[]>();
		ArrayList<ServerRecord> allHashes = new ArrayList<ServerRecord> ();

		getOrderedHashValues(keys, values, keysToHash, allHashes);

		for (Map.Entry<String, Byte[]> thisVal : keysToHash.entrySet()) {

			if(hashPassesZero == true){
				// the values to move are greater than min OR less than max

				// the value is less than max
				if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(maxHashByteArray)) < 0){
					keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
				}
				// the value is greater than min
				else if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(minHashByteArray)) > 0){
					keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
				}
			}
			else{
				// the values to move are greater than min AND less than max

				// the value is less than max
				if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(maxHashByteArray)) < 0){

					// the value is also greater than min
					if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(minHashByteArray)) > 0){
						keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
					}
				}
			}
		}



		// send RPCMethod.MoveData to server1, this will initiate the bulk transfer between server1 to server2
		server2.getIntraServerComms().call(
			"localhost:"+ (Integer.toString(port1)), 
			RPCMethod.MoveData, 
			Base64.getEncoder().encodeToString(byteMinHash), 
			Base64.getEncoder().encodeToString(byteMaxHash), 
			thisHost + ":" + port2) ;



		Thread.sleep(200);

		// check that the expected keys were moved
		for (int i = 0;  i < keys.length; ++i) {
			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}

			if(keysMovedToHash.containsKey(keys[i])){
				assertFalse(notHasKey); // has key
				assertTrue(kvGetResp.equals(values[i]));
			}
			else{
				assertTrue(notHasKey);
			}
		}


		server1.close();
		server1 = null;

		server2.close();
		server2 = null;

		store1.disconnect();
		store1 = null;

		store2.disconnect();
		store2 = null;
	}


	@Test
	public void testServerNotResponsible() throws Exception{
		// // start a zookeeper client for monitoring
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// // start the first kvserver, kvstore, and get the comms and hasher
		int port1 = 10009;
		int cacheSize = 10;
		int zkPort = 2181;
		String zkHost = "localhost";
		String cacheStrategy = "FIFO";

		String[] keys   = {"a","b","c","d","e","f","g","h","i","j", "k" };
		String[] values = {"1","2","3","4","5","6","7","8","9","10","11"};


		KVStore store1 = new KVStore(thisHost, port1);

		KVServer server1 = new KVServer(thisHost, port1, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		server1.run();
		server1.clearStorage();
		server1.start();

		Thread.sleep(200);

		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		// start the second kvserver, kvstore, and get the comms and hasher
		int port2 = 10010;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		
		KVStore store2 = new KVStore(thisHost, port2);

		server2.run();
		server2.clearStorage();
		server2.start();

		Thread.sleep(200);

		HashMap<String, Byte[]> keysToHash = new HashMap<String,Byte[]>();
		ArrayList<ServerRecord> allHashes = new ArrayList<ServerRecord> ();

		getOrderedHashValues(keys, values, keysToHash, allHashes);


		IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();

		ServerRecord server1Record = new ServerRecord(thisHost, port1);
		ServerRecord server2Record = new ServerRecord(thisHost, port2);

		// determine which one is first clockwise from 0 (center)
		boolean server1Smallest = false;
		if(comp.compare(server1Record, server2Record) < 0){
			server1Smallest = true;
		}
		else{
			server1Smallest = false;
		}

		for (Map.Entry<String, Byte[]> thisVal : keysToHash.entrySet()) {
			// determine which server should have the key

			boolean belongsTo1 = false;
			if(server1Smallest){

				// belongs to server 1 if the key is less than server1Record  
				// 	OR
				// greater than  server2Record

				// check if less than server1Record
				if(comp.compare(new ServerRecord(thisVal.getValue()), server1Record) < 0){
					belongsTo1 = true;
				}
				// check if greater than server2Record
				else if(comp.compare(new ServerRecord(thisVal.getValue()), server2Record) > 0){
					belongsTo1 = true;
				}
			}
			else{

				// belongs to server 1 if the key is less than server1Record  
				// 	AND
				// greater than  server2Record 

				// check if less than server1Record
				if(comp.compare(new ServerRecord(thisVal.getValue()), server1Record) < 0){
					// check if greater than server2Record
					 if(comp.compare(new ServerRecord(thisVal.getValue()), server2Record) > 0){
						belongsTo1 = true;
					}
				}
			}

			if(belongsTo1){

				KVMessage respGet1 = store1.get(thisVal.getKey());

				assertFalse(respGet1.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));

				// KVMessage respGet2 = store2.get(thisVal.getKey());

				// assertTrue(respGet2.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));
				// assertTrue(respGet2.getKey().equals(thisHost + ":" + Integer.toString(port1)));
				
			}
			else{
				KVMessage respGet2 = store2.get(thisVal.getKey());

				assertFalse(respGet2.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));

				// KVMessage respGet1 = store1.get(thisVal.getKey());

				// assertTrue(respGet1.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));
				// assertTrue(respGet1.getKey().equals(thisHost + ":" + Integer.toString(port2)));
			}
		}


		server1.close();
		server1 = null;

		server2.close();
		server2 = null;

		store1.disconnect();
		store1 = null;

		store2.disconnect();
		store2 = null;

	}


	@Test
	public void testAddServer() throws Exception{
		// // start a zookeeper client for monitoring
		ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

		// // start the first kvserver, kvstore, and get the comms and hasher
		int port1 = 10011;
		int cacheSize = 10;
		int zkPort = 2181;
		String zkHost = "localhost";
		String cacheStrategy = "FIFO";

		String[] keys   = {"a","b","c","d","e","f","g","h","i","j", "k" };
		String[] values = {"1","2","3","4","5","6","7","8","9","10","11"};


		KVServer server1 = openServerAndAddData(
			thisHost, 
			port1, 
			zkHost, 
			zkPort, 
			cacheSize,
			cacheStrategy,
			keys,
			values
			);

		KVStore store1 = new KVStore(thisHost, port1);

		int port2 = 10012;

		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		
		server2.run();
		server2.clearStorage();


		// check zookeeper still has only 1 node added to cluster
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

		KVStore store2 = new KVStore(thisHost, port2);

		// confirm no data is added in server2
		for(int i = 0; i < keys.length; i++){
			boolean notHasKey = false;
			String kvGetResp = null;
			try{
				kvGetResp = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKey = true;
			}
			assertTrue(notHasKey);
		}





		// get the upper and lower hash values to move
		ServerRecord txServer = new ServerRecord(null);
		List<Byte> minHash = new ArrayList<Byte>();
		List<Byte> maxHash = new ArrayList<Byte>();
		server2.getConsistentHasher().preAddServer(new ServerRecord(thisHost, port2), txServer, minHash, maxHash);

		// convert Byte[] to byte[] for ics2.call
		byte[] byteMinHash = new byte[minHash.size()];
		for(int i =0; i < minHash.size(); ++i)  byteMinHash[i] = minHash.get(i);

		byte[] byteMaxHash = new byte[maxHash.size()];
		for(int i =0; i < maxHash.size(); ++i)  byteMaxHash[i] = maxHash.get(i);


		// convert Byte[] to byte[] for ics2.call
		Byte[] minHashByteArray = new Byte[minHash.size()];
		for(int i =0; i < minHash.size(); ++i)  minHashByteArray[i] = minHash.get(i);

		Byte[] maxHashByteArray = new Byte[maxHash.size()];
		for(int i =0; i < maxHash.size(); ++i)  maxHashByteArray[i] = maxHash.get(i);



		IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();

		boolean hashPassesZero = false;
		if(comp.compare(new ServerRecord(maxHashByteArray), new ServerRecord(minHashByteArray)) < 0){ // max is less than min
			hashPassesZero = true;
		}

		// for the first 3 hashed values, record them for comparison later in get -> keysMovedToHash()
		HashMap<String, Byte[]> keysMovedToHash = new HashMap<String, Byte[]>();


		HashMap<String, Byte[]> keysToHash = new HashMap<String,Byte[]>();
		ArrayList<ServerRecord> allHashes = new ArrayList<ServerRecord> ();

		getOrderedHashValues(keys, values, keysToHash, allHashes);

		for (Map.Entry<String, Byte[]> thisVal : keysToHash.entrySet()) {

			if(hashPassesZero == true){
				// the values to move are greater than min OR less than max

				// the value is less than max
				if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(maxHashByteArray)) < 0){
					keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
				}
				// the value is greater than min
				else if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(minHashByteArray)) > 0){
					keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
				}
			}
			else{
				// the values to move are greater than min AND less than max

				// the value is less than max
				if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(maxHashByteArray)) < 0){

					// the value is also greater than min
					if(comp.compare(new ServerRecord(thisVal.getValue()), new ServerRecord(minHashByteArray)) > 0){
						keysMovedToHash.put(thisVal.getKey(), thisVal.getValue());
					}
				}
			}
		}



		// send RPCMethod.MoveData to server1, this will initiate the bulk transfer between server1 to server2
		server2.getIntraServerComms().call(
			"localhost:"+ (Integer.toString(port1)), 
			RPCMethod.MoveData, 
			Base64.getEncoder().encodeToString(byteMinHash), 
			Base64.getEncoder().encodeToString(byteMaxHash), 
			thisHost + ":" + port2) ;



		Thread.sleep(200);


		// use isc to start server 2
		server1.getIntraServerComms().call(
			"localhost:"+ (Integer.toString(port2)), 
			RPCMethod.Start
			);

		Thread.sleep(200);

		for (int i = 0; i < keys.length; ++i) {
			
			
			boolean notHasKeyServer1 = false;
			String kvGetResp1 = null;
			try{
				kvGetResp1 = server1.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKeyServer1 = true;
			}



			boolean notHasKeyServer2 = false;
			String kvGetResp2 = null;
			try{
				kvGetResp2 = server2.getKV(keys[i]);
			} catch(ICache.KeyDoesntExistException e){
				notHasKeyServer2 = true;
			}


			
			KVMessage respGet1 = store1.get(keys[i]);

			KVMessage respGet2 = store2.get(keys[i]);


			// means it should be in server 2 but not server 1 since it will be deleted from server 1
			// also check htat not responsible is thrown
			if(keysMovedToHash.containsKey(keys[i])){ 

				// // check that server 1 returns SERVER_NOT_RESPONSIBLE and redirects to correct server
				// assertTrue(respGet1.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));
				// assertTrue(respGet1.getKey().equals(thisHost + ":" + Integer.toString(port2)));

				// // assert that the value is deleted from server1 using the cache directly
				// assertTrue(notHasKeyServer1);

				// assert that the query succeeded for server 2 and returns the right value
				assertTrue(respGet2.getStatus().equals(StatusType.GET_SUCCESS));
				assertTrue(respGet2.getValue().equals(values[i]));

				// assert that the direct cache read was also successful for server 2
				assertFalse(notHasKeyServer2);
				
			}

			// otherwise it should be found in server 1
			else{

				// // check that server 1 returns SERVER_NOT_RESPONSIBLE and redirects to correct server
				// assertTrue(respGet2.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE));
				// assertTrue(respGet2.getKey().equals(thisHost + ":" + Integer.toString(port1)));

				// // assert that the value is deleted from server1 using the cache directly
				// assertTrue(notHasKeyServer2);

				// assert that the query succeeded for server 2 and returns the right value
				assertTrue(respGet1.getStatus().equals(StatusType.GET_SUCCESS));
				assertTrue(respGet1.getValue().equals(values[i]));

				// assert that the direct cache read was also successful for server 2
				assertFalse(notHasKeyServer1);
			}

		}





		server1.close();
		server1 = null;

		server2.close();
		server2 = null;

		store1.disconnect();
		store1 = null;

		store2.disconnect();
		store2 = null;
	}

	// @Test
	// public void testMoveDataAndStartServer(){
	// 	// start a zookeeper client for monitoring
	// 	ZooKeeper zk = new ZooKeeper(zkAddr, 1000, this);

	// 	// start the first kvserver, kvstore, and get the comms and hasher
	// 	int port1 = 10000;
	// 	KVServer server1 = new KVServer(thisHost, port1, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		

	// 	server1.run();
	// 	server1.clearStorage();

	// 	// check zookeeper has no nodes added to cluster yet
	// 	assertTrue(zk.getChildren(clusterGroup, false).size() == 0);
		
	// 	// check that a node is added in start
	// 	server1.start();
	// 	Thread.sleep(100);
	// 	assertTrue(zk.getChildren(clusterGroup, false).size() == 1);

	// 	// add data to the first server using a kvstore
	// 	KVStore store1 = new KVStore(thisHost, port1);
	// 	String[] keys = {"a","b","c","d","e","f","g","h","i","j", "k" };
	// 	String[] values={"1","2","3","4","5","6","7","8","9","10","11"};

	// 	assertTrue(keys.length == values.length);
	// 	for(int i = 0; i < keys.length; i++){
	// 		store1.put(keys[i], values[i]);
	// 	}

	// 	// confirm data is added
	// 	for(int i = 0; i < keys.length; i++){
	// 		KVMessage respGet = store1.get(keys[i]);
	// 		assertTrue(respGet.getStatus().equals(StatusType.GET_SUCCESS));
	// 		assertTrue(respGet.getValue().equals(values[i]));
	// 	}
	// }


}


