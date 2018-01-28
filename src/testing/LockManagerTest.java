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

				
				// use a thread to show that it fails after timeout when the lock is already aquired
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
	 *	Create 1000 threads and assert that they comlete in a reasonable time.
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
				finalTIDOrder = new ArrayList<Integer>();
				listOfEndtimes = new ArrayList<Integer>();

				hasLockOne = lockManager.getLock("a", Duration.ofSeconds(2));
				assertTrue(hasLockOne);
				
				longArray = new long[total];

				System.out.print("started id : " + currentTID + "\n");
				for (Integer i = 0; i < total; i++) {
					// use a thread to show that it succeeds right after the parent releases the lock.
					lockThreadSuccessful = false;

					Thread t = new Thread(new Thread() {
						private Integer tid = currentTID;
						@Override
						public void run() {
							try{
								Long startTimeMilli2 = new Date().getTime();
								lockThreadSuccessful = currentILock.getLock("a", Duration.ofSeconds(20));


								Long endTimeMilli2 = new Date().getTime();
								currentILock.releaseLock("a");
								Long LockThreadFailTime2 = endTimeMilli2 - startTimeMilli2;
								// System.out.print("out at : " + tid + lockThreadSuccessful + " endtime " + endTimeMilli2 + " total tiem " + LockThreadFailTime2 + "\n");

								finalTIDOrder.add(new Integer(tid));
								listOfEndtimes.add((int)(endTimeMilli2/1000));
								longArray[tid] = LockThreadFailTime2;

								
							} catch(Exception e){
							}
						}
					});

					SimpleEntry<Integer,Thread> newThreadEntry = new SimpleEntry<Integer,Thread>(currentTID, t);
					threadList.add(newThreadEntry);
					currentTID++;
				}
				

				System.out.print("final id : " + currentTID + "\n");

				Thread lastThread;
			    for(SimpleEntry<Integer,Thread> threadEntry : threadList){
			    	threadEntry.getValue().start();
			    	lastThread = threadEntry.getValue();
			    	// System.out.print("Order started : " + threadEntry.getKey() + "\n");

			    }

		        lockManager.releaseLock("a");
		        Long startTimeMilli3 = new Date().getTime();

		        System.out.print("start : " + startTimeMilli3 + "\n");

		        while(listOfEndtimes.size() < total){
		        	Thread.sleep(2);
		        	if(new Date().getTime() - startTimeMilli3 >  10000){
		        		break;
		        	}
		        }
		        

		        Long end3 = new Date().getTime();

		        System.out.print("end3 : " + end3 + "\n");


		        
		        assertTrue(listOfEndtimes.size() == total); // handles 1000 (or total) threads

		        for(Integer tidN : finalTIDOrder) {
		        	// System.out.print("Order compelted : " + tidN + "\n");
		        }

		        for(Integer theTime : listOfEndtimes) {
		        	// System.out.print("PRINTING time : " + theTime + "\n");
		        }

		        for(long ii : longArray){
		        	// System.out.print("PRINTING time 2 : " + ii + "\n");
		        }

		        // assertTrue(lockThreadSuccessful);
		        // assertTrue(LockThreadFailTime < 1000); // show that the timeout of 1s is reached
		       
		        // print LockThreadFailTime;

		        System.out.print("Order compelted : " + new Date().getTime() + "\n");
			
		    }
			    
		}
		catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}	
	}

	// also do for holding multiple locks, the synchronisation (when holding a lock with a queue, then releasing it, the next in queue gets it)

	public ILockManager currentILock;
	public boolean lockThreadSuccessful;
	public Long LockThreadFailTime;

	public ArrayList<Integer> finalTIDOrder;
	public Integer currentTID;
	public ArrayList<Integer> listOfEndtimes;

	public long[] longArray;
}


