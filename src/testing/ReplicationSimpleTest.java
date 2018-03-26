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

	public List<String> getServerMissingKeys(String[] keys, String[] values, KVServer server) {

		ArrayList<String> missingKeys = new ArrayList<String>();

		for (int i = 0; i < keys.length; ++i) {

			String resp = null;

			if(!server.inCache(keys[i])){
				missingKeys.add(keys[i]);
			}
			else{

				// check that it matches the value
				System.out.println("IN a kvServer: " + keys[i]);
				try{
					resp = server.getKV(keys[i]);
				}catch(ICache.KeyDoesntExistException e ){
					System.out.println("ReplicationSimpleTest expection: "+e.getMessage());
				} catch (ICache.StorageException e){
					System.out.println("STORAGE EXCEPTION");
				}
				catch (Exception e){
					System.out.println("1 EXCEPTION");
				}
				
				assertTrue(resp.equals(values[i]));
			}
		}
		return missingKeys;
	}


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

		int replication = 2;

		int port1 = 20100;
		KVServer server1 = new KVServer(thisHost, port1, "localhost", 2181, 10, "FIFO", replication);
		server1.run();
		server1.clearStorage();
		server1.start();

		Thread.sleep(200);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 1);




		String[] keys = {"a","b","c","d","e","f","g","h","i","j", "k" , "l", "m", "n", "o", "p", "q", "r", "s", "t", "u"};
		String[] values={"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17", "18","19","20","21"};


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



		for (int i = 0; i < keys.length; ++i) {


			String getRes = server1.getKV(keys[i]);
			assertTrue(getRes.equals(values[i]));

		}


		int port2 = 20101;
		KVServer server2 = new KVServer(thisHost, port2, "localhost", 2181, 10, "FIFO", replication);
		server2.run();
		// server2.clearStorage();
		server2.start();

		Thread.sleep(2000);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 2);
		// add all these key values



		int totalMissingBoth = 0;
		int totalNull1 = 0;
		int totalNull2 = 0;



		int getkv_no_key_1 = 0;
		int getkv_no_key_2 = 0;
		for (int i = 0; i < keys.length; ++i) {
			boolean missingEither = false;


			try{
				String get1 = server1.getKV(keys[i]);
				if(get1 == null){
					totalNull1++;
				}
			} catch(ICache.KeyDoesntExistException e ){
				getkv_no_key_1++;
				System.out.println(port1+"aaaaaaaaa\n\n\n=====keys[i]:::values[i]"+keys[i]+":::"+values[i]);
			}

			try{
				String get2 = server2.getKV(keys[i]);
				if(get2 == null){
					totalNull2++;
				}
			}catch(ICache.KeyDoesntExistException e){
				System.out.println(port2+"aaaaaaaaa\n\n\n=====keys[i]:::values[i]"+keys[i]+":::"+values[i]);
				getkv_no_key_2++;
			}

		}


		assertTrue(totalNull1 == 0);
		assertTrue(totalNull2 == 0);

		assertTrue(getkv_no_key_1 == 0);
		assertTrue(getkv_no_key_2 == 0);

		System.out.println("totalNull1:"+totalNull1);
		System.out.println("totalNull2:"+totalNull2);

		System.out.println("getkv_no_key_1:"+getkv_no_key_1);
		System.out.println("getkv_no_key_2aaaaaaaaaaaa\n\n\n\n\n\n\n:"+getkv_no_key_2);





		// start

		// assertTrue(totalMissingBoth == 0);
		// assertTrue(totalMissing2 == 0);
		// assertTrue(totalMissing1 == 0);
		// if(true){return;}


		// check that both servers have a replica (factor = 1)

		// int totalMissing1 = 0;
		// int totalMissing2 = 0;
		
		// for (int i = 0; i < keys.length; ++i) {
			
		// 	boolean missingOne = false;
		// 	boolean missingTwo = false;

		// 	String getRes1 = server1.getKV(keys[i]);
		// 	if(getRes1 == null){
		// 		totalMissing1++;
		// 		missingOne= true;
		// 	}

		// 	String getRes2 = server2.getKV(keys[i]);
		// 	if(getRes2 == null){
		// 		if(missingOne == true){
		// 			assertTrue(false);
		// 		}
		// 		totalMissing2++;
		// 		missingOne = true;
		// 	}
		// 	assertTrue(missingOne);
		// }
		// assertTrue(totalMissing1 == 0);
		// assertTrue(totalMissing2 == 0);

		// server1.stop();

		// Thread.sleep(500);

		// int i = 0;
		// for(String key : keys){
		// 	KVMessage respGet = store1.get(keys[i]);
		// 	assertTrue(respGet.getStatus().equals(StatusType.GET_SUCCESS));
		// 	assertTrue(respGet.getValue().equals(values[i]));

		// 	i++;
		// }

		int port3 = 20102;
		KVServer server3 = new KVServer(thisHost, port3, "localhost", 2181, 10, "FIFO", replication);
		server3.run();
		server3.clearStorage();
		server3.start();

		Thread.sleep(2000);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 3);



		 totalMissingBoth = 0;
		 totalNull1 = 0;
		 totalNull2 = 0;



		 getkv_no_key_1 = 0;
		 getkv_no_key_2 = 0;
		for (int i = 0; i < keys.length; ++i) {
			boolean missingEither = false;


			try{
				String get1 = server1.getKV(keys[i]);
				if(get1 == null){
					totalNull1++;
				}
			} catch(ICache.KeyDoesntExistException e ){
				getkv_no_key_1++;
			}

			try{
				String get2 = server2.getKV(keys[i]);
				if(get2 == null){
					totalNull2++;
				}
			}catch(ICache.KeyDoesntExistException e){
				System.out.println("keys[i]:::values[i]"+keys[i]+":::"+values[i]);
				getkv_no_key_2++;
			}

		}


		// assertTrue(totalNull1 >0);
		// assertTrue(totalNull2 > 0);

		// assertTrue(getkv_no_key_1 > 0);
		// assertTrue(getkv_no_key_2 >  0);
		
		System.out.println("totalNull1:"+totalNull1);
		System.out.println("totalNull2:"+totalNull2);

		System.out.println("getkv_no_key_1:"+getkv_no_key_1);
		System.out.println("aaagetkv_no_key_2:"+getkv_no_key_2);
		// // check that not all data is on server1 and server2, but the keys that are found on them should have some missing




		int port4 = 20103;
		KVServer server4 = new KVServer(thisHost, port4, "localhost", 2181, 10, "FIFO", replication);
		server4.run();
		server4.clearStorage();
		server4.start();

		Thread.sleep(2000);
		assertTrue(zk.getChildren(clusterGroup, false).size() == 4);


		server1.stop();
		Thread.sleep(1000);
		server2.stop();
		Thread.sleep(1000);
		server3.stop();
		Thread.sleep(1000);


		KVStore store4 = new KVStore(thisHost, port4);


		for (int i =0; i < keys.length; ++i) {

			KVMessage resp0 = store4.get(keys[i]);
			assertTrue(resp0 != null);
			System.out.println(resp0.getStatus());
			System.out.println("THE GET : "+resp0.getStatus());
			assertTrue(resp0.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(resp0.getValue().equals(values[i]));
		}

			
		// List<String> missingKeysFrom1And2 = new ArrayList<String>();
		// for (int i = 0; i < keys.length; ++i) {
		// 	// check the key is missing from 1


		// 	System.out.println("KEY : " + keys[i]);
		// 	if(!server1.inCache(keys[i])){

		// 		System.out.println("KEY not 1 : " + keys[i]);
		// 		// check the key is missing from 2
		// 		if(!server2.inCache(keys[i])) {

		// 			System.out.println("KEY not both : " + keys[i]);
		// 			missingKeysFrom1And2.add(keys[i]);
		// 			continue;
		// 		}
		// 	}
		// }

		// assertTrue(missingKeysFrom1And2.size() > 0);



		// // stop a server
		// server3.stop();

		// Thread.sleep(400);

		// List<String> missingKeysFrom1And2_2 = new ArrayList<String>();
		// for (int i = 0; i < keys.length; ++i) {
		// 	// check the key is missing from 1


		// 	System.out.println("KEY : " + keys[i]);
		// 	if(!server1.inCache(keys[i])){

		// 		System.out.println("KEY not 1 : " + keys[i]);
		// 		// check the key is missing from 2
		// 		if(!server2.inCache(keys[i])) {

		// 			System.out.println("KEY not both : " + keys[i]);
		// 			missingKeysFrom1And2_2.add(keys[i]);
		// 			continue;
		// 		}
		// 	}
		// }

		// assertTrue(missingKeysFrom1And2_2.size() == 0);





	}


}



