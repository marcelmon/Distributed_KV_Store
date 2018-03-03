package testing;

import org.junit.*;

import app_kvServer.KVServer;
import client.KVStore;

import junit.framework.TestCase;


import common.messages.Message.StatusType;

import common.messages.*;

public class LockWriteKVServerTest extends TestCase {
	protected KVServer server;
	protected KVStore store;

	@Override
	public void setUp() throws Exception {

		int port = 50000;
		server = new KVServer("", port, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		store = new KVStore("localhost", port);
		server.run();
		server.clearStorage();
	}
	
	@Override
	public void tearDown() {
		store.disconnect();
		server.close();
	}


	/*
	
	sanity-check : test put operations
		Includes : 
			PUT
			UPDATE
			DELETE

	*/
	@Test
	public void testSanityCheck(){

		
		server.clearStorage();

		try {
			KVMessage respGet0 = store.get("a");
			assertTrue(respGet0.getStatus().equals(StatusType.GET_ERROR));
		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}

		try {
			KVMessage respPut1 = store.put("a", "1");
			assertTrue(respPut1.getStatus().equals(StatusType.PUT_SUCCESS));	

		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}

		try {
			KVMessage respGet1 = store.get("a");
			assertTrue(respGet1.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(respGet1.getValue().equals("1"));
		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}


		try {
			KVMessage respPut2 = store.put("a", "2");
			assertTrue(respPut2.getStatus().equals(StatusType.PUT_UPDATE));
		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}


		try {
			KVMessage respGet2 = store.get("a");
			assertTrue(respGet2.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(respGet2.getValue().equals("2"));
		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}


		try {
			KVMessage respPut3 = store.put("a", "");
			assertTrue(respPut3.getStatus().equals(StatusType.DELETE_SUCCESS));
		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}


		try {
			KVMessage respGet3 = store.get("a");
			assertTrue(respGet3.getStatus().equals(StatusType.GET_ERROR));
		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}
	}



	public void putValues(String[] keys, String[] values){
		assertTrue(keys.length == values.length);
		for (int i = 0; i < keys.length; ++i) {

			try {
				KVMessage respPut = store.put(keys[i], values[i]);
				assertTrue(respPut.getStatus().equals(StatusType.PUT_SUCCESS));
			} catch (Exception e){
				System.out.println("Exception : " + e.getMessage());
			}

			try {
				KVMessage respGet = store.get(keys[i]);
				assertTrue(respGet.getStatus().equals(StatusType.GET_SUCCESS));
				assertTrue(respGet.getValue().equals(values[i]));
			} catch (Exception e){
				System.out.println("Exception : " + e.getMessage());
			}
		}
	}


	@Test
	public void testLockWrite(){

		try {
			server.clearStorage();

			// check that key does not exist
			KVMessage respGet = store.get("a");
			assertTrue(respGet.getStatus().equals(StatusType.GET_ERROR));
		

			// get write lock
			server.lockWrite();

			// assert that write lock works
			KVMessage resp1 = store.put("a", "1");
			assertTrue(resp1.getStatus().equals(StatusType.SERVER_WRITE_LOCK));

			server.unlockWrite();


			KVMessage resp2 = store.put("a", "2");
			assertTrue(resp2.getStatus().equals(StatusType.PUT_SUCCESS));
	

			KVMessage respGet2 = store.get("a");
			assertTrue(respGet2.getStatus().equals(StatusType.GET_SUCCESS));
			assertTrue(respGet2.getValue().equals("2"));

		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}
	}


	/*
		the variable pendingPuts should be incremented for each put that is to be serviced, 
		and decremented when its complete.

		This test check this by using 1 thread to constantly poll the value at server, 
		and another set of threads to perform put operations.

		NOTE: The OnKVMsgRcd in KVServer is synchronized, so only 1 can even access at a time.

		Warning: the thread checking for pending could be delayed and not poll the value fast enough.
			To prevent this, we start several threads so that at least will be be pending while it polls.
	*/
	@Test
	public void testPendingPutsIncrements(){
		try {
			server.clearStorage();

			Thread getMaxPut;
			RunGetMaxPendingPuts getMaxPendingPutsRunnable = new RunGetMaxPendingPuts(server);
			getMaxPut = new Thread(getMaxPendingPutsRunnable);

			Thread put1;
			String key1 = "a";
			String val1 = "1";
			put1 = new Thread(new RunPut(key1, val1, store));

			Thread put2;
			String key2 = "b";
			String val2 = "2";
			put2 = new Thread(new RunPut(key2, val2, store));

			Thread put3;
			String key3 = "c";
			String val3 = "3";
			put3 = new Thread(new RunPut(key3, val3, store));

			getMaxPut.start();

			// show there are currently no pending puts and also the max (from thread) is 0
			assertTrue(server.getPendingPuts() == 0);
			assertTrue(getMaxPendingPutsRunnable.getMaxPendingPuts() == 0);

			// start the put threads and wait for them to finish
			put1.start();
			put2.start();
			put3.start();
			while(put3.isAlive() || put2.isAlive() || put1.isAlive()){
				Thread.sleep(10);
			}

			// check that the max was in fact >= 1 but has since returned to 0 (the put completed)
			assertTrue(getMaxPendingPutsRunnable.getMaxPendingPuts() >= 1);
			assertTrue(server.getPendingPuts() == 0);

			// interupt, thus stopping, the thread and join to it
			getMaxPendingPutsRunnable.setInterupt();
			getMaxPut.join();

		} catch (Exception e){
			System.out.println("Exception : " + e.getMessage());
		}
	}


	/*
		When pending puts > 0 then getting the write lock should block until they are finished.

		Manually increment the pending puts, try getting the write lock in another thread and show that it must wait.
	*/
	@Test
	public void testPendingPutsBlockingWriteLock(){
		Thread getWriteLock;
		GetWriteLockRunnable getWriteLockRunnable = new GetWriteLockRunnable(server);
		getWriteLock = new Thread(getWriteLockRunnable);

		// first increment pending puts manually
		server.incrementPendingPuts();

		// run the get write lock, it will be blocked in run() until the pending puts == 0
		getWriteLock.start();
		try{

			// show that the thread is alive only as long as the pending puts is > 0
			Thread.sleep(100);
			assertTrue(getWriteLock.isAlive());

			Thread.sleep(100);
			assertTrue(getWriteLock.isAlive());

			Thread.sleep(100);
			assertTrue(getWriteLock.isAlive());

			server.decrementPendingPuts();
			Thread.sleep(100);

			assertFalse(getWriteLock.isAlive());

			// assert that write lock works
			try {
				KVMessage resp1 = store.put("a", "1");
				assertTrue(resp1.getStatus().equals(StatusType.SERVER_WRITE_LOCK));
			} catch (Exception e){
				System.out.println("Exception " + e.getMessage());
			}
				


		} catch (InterruptedException e){
			System.out.println("Exception " + e.getMessage());
		}
		
	}

	public class GetWriteLockRunnable implements Runnable {

		KVServer kvServer;
		public GetWriteLockRunnable(KVServer kvServer){
			this.kvServer = kvServer;
		}

		@Override
		public void run() {
			// will only finish once it has the write lock
			kvServer.lockWrite();
		}

	}

	// @Test
	// public void testPendingPutsBlocks(){

	// 	Thread put1;
	// 	Thread put2;

	// 	Thread getMaxPut;

	// 	try {
	// 		server.clearStorage();

	// 		String key1 = "a";
	// 		String val1 = "1";
	// 		put1 = new Thread(new RunPut(key1, val1, store));


	// 		String key2 = "b";
	// 		String val2 = "2";
	// 		put2 = new Thread(new RunPut(key2, val2, store));

	// 		RunGetMaxPendingPuts getMaxPendingPutsRunnable = new RunGetMaxPendingPuts(server);
	// 		getMaxPut = new Thread(getMaxPendingPutsRunnable);

	// 		getMaxPut.start();

	// 		put1.start();
	// 		put2.start();

	// 		while(put2.isAlive() || put1.isAlive()){
	// 			Thread.sleep(10);
	// 		}

	// 		int maxInterupt = getMaxPendingPutsRunnable.getMaxPendingPuts();
	// 		getMaxPendingPutsRunnable.setInterupt();
	// 		getMaxPut.join();

	// 		System.out.println("OUT WITH MAX PENDING :: " + maxInterupt);

	// 	} catch (Exception e){
	// 		System.out.println("Exception : " + e.getMessage());
	// 	}
	// }

	public class RunGetMaxPendingPuts implements Runnable {
		
		boolean isInterupt = false;

		KVServer kvServer;

		private volatile int maxPendingPuts = 0;

		public RunGetMaxPendingPuts(KVServer kvServer){
			this.kvServer = kvServer;
		}

		@Override
		public void run() {
			while(!isInterupt){
				int pendingPuts = kvServer.getPendingPuts();
				if(maxPendingPuts < pendingPuts){
					maxPendingPuts = pendingPuts;
				}
			}
		}

		public void setInterupt(){
			isInterupt = true;
		}

		public int getMaxPendingPuts(){
			return maxPendingPuts;
		}
	}


	public class RunPut implements Runnable {
		
		String key;
		String value;
		KVStore clientStore;

		public RunPut(String key, String value, KVStore clientStore){
			this.key = key;
			this.value = value;
			this.clientStore = clientStore;
		}

		@Override
		public void run() {
			try{
				KVMessage resp = store.put(key, value);
				assertTrue(resp.getStatus().equals(StatusType.PUT_SUCCESS));
			} catch (Exception e){
				System.out.println("Exception : " + e.getMessage());
			}
			
		}
	}
}