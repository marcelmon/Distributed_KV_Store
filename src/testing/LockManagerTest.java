package testing;

import java.util.Iterator;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;

import java.util.Random;


import java.io.*;

import java.util.Date;


import java.util.Arrays;

import java.util.Objects;

import java.time.Duration;

import java.lang.Integer;

public class LockManagerTest extends TestCase {
	protected ILockManager[] lockManagers;
	public ILockManager currentILock;
	public boolean lockThreadSuccessful;
	public Long LockThreadFailTime;

	public Integer currentTID;


	public ArrayList startAndEndTimesByThreadId;
	public ArrayList failedStartAndEndTimesByThreadId;


	public long[] longArray;

	@Override
	public void setUp() {

		
		lockManagers = new ILockManager[1];
		try{
			lockManagers[0] = new KeyLockManager();
		}
		catch(Exception e){
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testGetLock() {
		Long startTimeMilli; 
		Long endTimeMilli; 

		try {
			boolean hasLockOne = false;
			boolean hasLockTwo = false;
			
			for (ILockManager lockManager : lockManagers) {
				currentILock = lockManager;

				// Check that can't release a lock we don't have
				boolean caught = false;
				try {
					lockManager.releaseLock("a");

				} catch (ILockManager.LockNotHeldException e) {
					caught = true;
				}
				assertTrue(caught);

				// check getting 1 lock happens in expected time
				startTimeMilli = new Date().getTime();
				hasLockOne = lockManager.getLock("a", Duration.ofSeconds(2));
				endTimeMilli = new Date().getTime();

				assertTrue(hasLockOne);
				assertTrue(endTimeMilli - startTimeMilli < 500);

				// check getting same lock throws error
				caught = false;
				try {
					startTimeMilli = new Date().getTime();
					hasLockTwo = lockManager.getLock("a", Duration.ofSeconds(2));
					endTimeMilli = new Date().getTime();

				} catch (ILockManager.LockAlreadyHeldException e) {
					caught = true;
				}
				assertTrue(caught);
				assertTrue(endTimeMilli - startTimeMilli < 500);

				// use a thread to show that it fails after timeout when the lock is already acquired
				lockThreadSuccessful = true;

				Thread t = new Thread(new Thread() {
					@Override
					public void run() {
						try{
							Long startTimeMilli = new Date().getTime();
							lockThreadSuccessful = currentILock.getLock("a", Duration.ofSeconds(1));
							Long endTimeMilli = new Date().getTime();
							LockThreadFailTime = endTimeMilli - startTimeMilli;
						} catch(Exception e){
						}
					}
				});

		        t.start();
		        t.join();
		        assertFalse(lockThreadSuccessful);
		        assertTrue(LockThreadFailTime >= 1000); // show that the timeout of 1s is reached

		        lockManager.releaseLock("a"); // release the lock
			}
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}


	/*
	 *	Create 1000 threads and assert that they complete in a reasonable time.
	 *
	 */
	@Test
	public void testLockSwitchingThreads(){
		Long startTimeMilli; 
		Long endTimeMilli; 

		Integer total = 1000;

		try {
			ArrayList<SimpleEntry<Integer,Thread>> threadList = new ArrayList<SimpleEntry<Integer,Thread>>();

			final Long startTimeOverall = new Date().getTime();
			for (ILockManager lockManager : lockManagers) {
				
				currentILock = lockManager;

				boolean hasLockOne = false;
				boolean hasLockTwo = false;
				
				currentTID = new Integer(0);

				startAndEndTimesByThreadId = new ArrayList<SimpleEntry<Integer, SimpleEntry<Long, Long>>>();
				failedStartAndEndTimesByThreadId = new ArrayList<SimpleEntry<Integer, SimpleEntry<Long, Long>>>();

				hasLockOne = lockManager.getLock("a", Duration.ofSeconds(2));
				assertTrue(hasLockOne);
				


				for (Integer i = 0; i < total; i++) {
					// use a thread to show that it succeeds right after the parent releases the lock.

					Thread t = new Thread(new Thread() {
						private Integer tid = currentTID;
						@Override
						public void run() {
							try{
								// record time started waiting for lock (just after thread.start())
								Long threadStartTime = new Date().getTime();
								// get lock waiting for success or timeout (in seconds)
								boolean gotLock = currentILock.getLock("a", Duration.ofSeconds(20));
								// record time where lock was aquired
								Long threadEndTime = new Date().getTime();
								// put the timing information in the ArrayList while the lock is still squired to prevent other threads from doing the same at the same time
								if(gotLock){
									SimpleEntry<Long, Long> startAndEntTime = new SimpleEntry<Long, Long>(threadStartTime, threadEndTime);
									SimpleEntry<Integer, SimpleEntry<Long, Long>> timesByTID = new SimpleEntry<Integer, SimpleEntry<Long, Long>>(tid, startAndEntTime);
									startAndEndTimesByThreadId.add(timesByTID);
								}
								else{
									SimpleEntry<Long, Long> startAndEntTime = new SimpleEntry<Long, Long>(threadStartTime, threadEndTime);
									SimpleEntry<Integer, SimpleEntry<Long, Long>> timesByTID = new SimpleEntry<Integer, SimpleEntry<Long, Long>>(tid, startAndEntTime);
									failedStartAndEndTimesByThreadId.add(timesByTID);
								}
									
								currentILock.releaseLock("a");	

							} catch(Exception e){
								
							}
						}
					});

					SimpleEntry<Integer,Thread> newThreadEntry = new SimpleEntry<Integer,Thread>(currentTID, t);
					threadList.add(newThreadEntry);
					currentTID++;
				}

				Thread lastThread;
			    for(SimpleEntry<Integer,Thread> threadEntry : threadList){
			    	threadEntry.getValue().start();
			    	lastThread = threadEntry.getValue();
			    }

			    Long unlockStartTime = new Date().getTime();

		        lockManager.releaseLock("a");
		        
		        // sum of successes and failures
		        while(startAndEndTimesByThreadId.size() + failedStartAndEndTimesByThreadId.size() < total){
		        	Thread.sleep(2); // 2 miliseconds
		        	if(new Date().getTime() - unlockStartTime >  20000){
		        		break;
		        	}
		        }
		        
		        assertTrue(startAndEndTimesByThreadId.size() == total); // handles 1000 (or total) threads

		        // System.out.println("FINAL DATA : \n");
		        // for (int i = 0; i < startAndEndTimesByThreadId.size(); ++i) {
		        // 	 System.out.println(startAndEndTimesByThreadId.get(i).toString());
		        // }
		    }
			    
		}
		catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
			System.out.println();
		}	
	}

	// also do for holding multiple locks, the synchronisation (when holding a lock with a queue, then releasing it, the next in queue gets it)
}


