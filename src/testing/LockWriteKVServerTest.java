package testing;

import org.junit.*;

import app_kvServer.KVServer;
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

import java.util.List;
public class LockWriteKVServerTest extends TestCase {
	protected KVServer server;
	protected KVStore store;


	@Override
	public void setUp() throws Exception {

		int port = 5000;
		server = new KVServer("localhost", port, "localhost", 2181, 10, "FIFO"); // TODO put proper args when zookeeper implemented
		server.run();
		server.clearStorage();
		server.start();
		Thread.sleep(500);
		store = new KVStore("localhost", port);
		
	}
	
	@Override
	public void tearDown() {
		store.disconnect();
		server.close();
		server = null;
	}




	@Test
	public void testLockUnlockWriteInner(){

		// check no lockWrite() called
		assertFalse(server.lockWrite.isLockWrite());
		// check that lockWrite() gets lock
		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite());

		// test unlockWrite() works to unlock
		server.lockWrite.unlockWrite();
		assertFalse(server.lockWrite.isLockWrite());

		// no change
		server.lockWrite.unlockWrite();
		assertFalse(server.lockWrite.isLockWrite()); // unlocked



		// get multiple locks
		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite());
		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite());

		// unlock multiple times
		server.lockWrite.unlockWrite();
		assertTrue(server.lockWrite.isLockWrite()); // still has the lock (was locked twice)

		server.lockWrite.unlockWrite();
		assertFalse(server.lockWrite.isLockWrite()); // now unlocked


	}


	@Test
	public void testTestLockWrite(){

		assertFalse(server.lockWrite.isLockWrite()); // unlocked


		// testLockWrite() can only be used if the lock is not already locked (only 1 time)
		assertTrue(server.lockWrite.testLockWrite());
		assertTrue(server.lockWrite.isLockWrite());

		server.lockWrite.unlockWrite();
		assertFalse(server.lockWrite.isLockWrite()); // unlocked


		assertTrue(server.lockWrite.testLockWrite());
		assertTrue(server.lockWrite.isLockWrite());
		assertFalse(server.lockWrite.testLockWrite());
		assertTrue(server.lockWrite.isLockWrite());

		server.lockWrite.unlockWrite();
		assertFalse(server.lockWrite.isLockWrite()); // only 1 test lock write works
		
	}

	@Test
	public void testTestUnlockWrite(){
		
		assertFalse(server.lockWrite.isLockWrite()); // unlocked

		// testUnlockWrite() can only be used if the lock is locked and ONLY 1 time
		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite()); // locked
		assertTrue(server.lockWrite.testUnlockWrite()); // first time should be successful
		assertFalse(server.lockWrite.isLockWrite()); // unlocked

		assertFalse(server.lockWrite.testUnlockWrite());
		assertFalse(server.lockWrite.isLockWrite()); // unlocked

		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite()); // locked
		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite()); // locked (twice)
		assertFalse(server.lockWrite.testUnlockWrite()); // first time should be unsuccessful
		assertTrue(server.lockWrite.isLockWrite()); // still locked
		assertTrue(server.lockWrite.testUnlockWrite()); // second time successful
		assertFalse(server.lockWrite.isLockWrite()); // unlocked

	}


	

	/*
		Test that pending puts increments and decrements
		Test that pending puts is blocked by a lock
	*/
	@Test
	public void testPendingPutsInner(){

		assertTrue(server.lockWrite.getPendingPuts() == 0);
		server.lockWrite.incrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		server.lockWrite.incrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 2);
		server.lockWrite.incrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 3);

		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 2);
		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 0);

		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 0); // no lower than 0
		server.lockWrite.testIncrementPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 0);

	}

	@Test
	public void testTestPendingPutsInner(){
		assertTrue(server.lockWrite.getPendingPuts() == 0);
		assertFalse(server.lockWrite.isLockWrite()); // unlocked

		assertTrue(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		assertTrue(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 2);
		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 0);

		server.lockWrite.lockWrite();
		assertTrue(server.lockWrite.isLockWrite()); // locked
		assertFalse(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 0);
		server.lockWrite.unlockWrite();
		assertFalse(server.lockWrite.isLockWrite()); // unlocked
		assertTrue(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 0);

	}



	public class InnerGetWriteLockRunnable implements Runnable {

		KVServer.LockWrite lockWrite;
		public InnerGetWriteLockRunnable(KVServer.LockWrite lockWrite){
			this.lockWrite = lockWrite;
		}

		public void run(){
			lockWrite.lockWrite();
		}
	}


	// Test that when we have a pending put in queue that it blocks the locking for that time
	@Test
	public void testPendingPutsBlocksLock(){

		Thread innerGetWriteLockThread;
		InnerGetWriteLockRunnable innerGetWriteLockRunnable = new InnerGetWriteLockRunnable(server.lockWrite);
		innerGetWriteLockThread = new Thread(innerGetWriteLockRunnable);


		assertTrue(server.lockWrite.getPendingPuts() == 0);
		assertFalse(server.lockWrite.isLockWrite()); // unlocked

		assertTrue(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 1);

		assertTrue(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 2);



		assertFalse(server.lockWrite.isLockWrite());
		innerGetWriteLockThread.start();

	// show that the thread is alive only as long as the pending puts is > 0
		try{
			Thread.sleep(200); // should be enough time for the thread to have gotten the lock
		} catch(InterruptedException e){

		}
	
		assertTrue(innerGetWriteLockThread.isAlive());
		assertTrue(server.lockWrite.isLockWrite()); // thread already has the lock

		server.lockWrite.decrementPendingPuts();
		assertTrue(server.lockWrite.getPendingPuts() == 1);
		
		try{
			Thread.sleep(100); // should be enough time for the thread to have gotten the lock
		} catch(InterruptedException e){

		}
		assertTrue(innerGetWriteLockThread.isAlive());
		assertTrue(server.lockWrite.isLockWrite()); // should not yet have the lock

		server.lockWrite.decrementPendingPuts(); // now all have been decremented
		assertTrue(server.lockWrite.getPendingPuts() == 0);

		try{
			Thread.sleep(100); // should be enough time for the thread to have gotten the lock
		} catch(InterruptedException e){

		}

		assertFalse(innerGetWriteLockThread.isAlive()); // thread has finally been able to exit
		assertTrue(server.lockWrite.isLockWrite()); // should now have the lock

		assertFalse(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 0);

		server.lockWrite.unlockWrite(); // now unlocked
		assertFalse(server.lockWrite.isLockWrite());

		assertTrue(server.lockWrite.testIncrementPuts());
		assertTrue(server.lockWrite.getPendingPuts() == 1);

		server.lockWrite.decrementPendingPuts(); // now all have been decremented
		assertTrue(server.lockWrite.getPendingPuts() == 0);
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
		try{
			Thread.sleep(100);	
		}
		catch(InterruptedException e){

		}
		
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


	@Test
	public void testServerLockWrite(){

		try {
			server.clearStorage();
			Thread.sleep(100);
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
	public void testServerPendingPutsIncrements(){
		try {
			server.clearStorage();

			Thread getMaxPut;
			GetMaxPendingPutsRunnable getMaxPendingPutsRunnable = new GetMaxPendingPutsRunnable(server.lockWrite);
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
			assertTrue(server.lockWrite.getPendingPuts() == 0);
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
			assertTrue(server.lockWrite.getPendingPuts() == 0);

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
	public void testServerPendingPutsBlockingWriteLock(){
		Thread getWriteLock;
		ServerGetWriteLockRunnable getWriteLockRunnable = new ServerGetWriteLockRunnable(server);
		getWriteLock = new Thread(getWriteLockRunnable);

		// first increment pending puts manually
		server.lockWrite.testIncrementPuts();

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

			server.lockWrite.decrementPendingPuts();
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

	public class ServerGetWriteLockRunnable implements Runnable {

		KVServer kvServer;
		public ServerGetWriteLockRunnable(KVServer kvServer){
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

	public class GetMaxPendingPutsRunnable implements Runnable {
		
		public boolean isInterupt = false;

		public KVServer.LockWrite lockWrite;

		private volatile int maxPendingPuts = 0;

		public GetMaxPendingPutsRunnable(KVServer.LockWrite lockWrite){
			this.lockWrite = lockWrite;
		}

		@Override
		public void run() {
			while(!isInterupt){
				int pendingPuts = lockWrite.getPendingPuts();
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
		KVStore store;

		public RunPut(String key, String value, KVStore store){
			this.key = key;
			this.value = value;
			this.store = store;
		}

		@Override
		public void run() {
			try{

				ZooKeeper zk = new ZooKeeper("localhost:2181", 100, null);
				List<String> servers = zk.getChildren("/cluster", false);		
				for(String s:servers){
					System.out.println("SERVER : " + s);
				}


				KVMessage resp = store.put(key, value);
				System.out.println("PUT STATUS : " + resp.getStatus()+"\n\n\n\n\n\n");
				assertTrue(resp.getStatus().equals(StatusType.PUT_SUCCESS));
			} catch (Exception e){
				System.out.println("Exception : " + e.getMessage());
			}
			
		}
	}
}