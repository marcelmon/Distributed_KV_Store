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
				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						try{
							Long startTimeMilli = new Date().getTime();
							lockThreadSuccessful = currentILock.getLock("a", Duration.ofSeconds(2));
							Long endTimeMilli = new Date().getTime();
							LockThreadFailTime = endTimeMilli - startTimeMilli;
						} catch(Exception e){
						}
					}
				});

		        t.start();
		        t.join();
		        assertFalse(lockThreadSuccessful);
		        assertTrue(LockThreadFailTime >= 2000);
		       

		        lockManager.releaseLock("a"); // release the lock

			}
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}

	// also do for holding multiple locks, the synchronisation (when holding a lock with a queue, then releasing it, the next in queue gets it)

	public ILockManager currentILock;
	public boolean lockThreadSuccessful;
	public Long LockThreadFailTime;


}


