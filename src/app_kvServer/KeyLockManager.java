

package app_kvServer;

import java.util.concurrent.locks.ReentrantLock;

import java.util.LinkedList;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.util.Arrays;

import java.util.Objects;


import java.time.Duration;

import java.util.Date;

import java.util.concurrent.TimeUnit;


public class KeyLockManager implements ILockManager {


	protected HashMap<String, SimpleEntry<ReentrantLock, Integer>> keyLocks;


	protected ReentrantLock tableLock;

	private int activeKeyLocks;


	public KeyLockManager() {
		
		activeKeyLocks = 0;
		tableLock = new ReentrantLock(true);

		keyLocks = new HashMap<String, SimpleEntry<ReentrantLock, Integer>>();
		
	}




	private boolean getTableLock(Duration timeout) {
		try{
			boolean lockAquired = tableLock.tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS);
			if(lockAquired) {
				return true;
			}
		}
		catch(InterruptedException e){

		}
		return false;
	}

	/*
	 * To get a lock, first get lock on table (all lock updates), see if already has the lock, 
	 * see if there is no one else locking, otherwise add the thread to the queue and trylock() with timeout.
	 *
	 */

	@Override
	public boolean getLock(String key, Duration timeout) throws LockAlreadyHeldException {
		if(!getTableLock(timeout)){
			return false;
		}

		SimpleEntry<ReentrantLock, Integer> keyLockRow = keyLocks.get(key);
		if(keyLockRow == null){ // no current lock on this key

			ReentrantLock keyLock = new ReentrantLock(true); // Build a new lock for this key
			keyLock.lock(); // aquire the lock immediately
			keyLocks.put(key, new SimpleEntry<ReentrantLock, Integer>(keyLock, 1)); // set intial count to 1
			tableLock.unlock();
			return true;
		}

		if(keyLockRow.getKey().isHeldByCurrentThread()){
			tableLock.unlock();
			throw new LockAlreadyHeldException("Already have lock " + key);
		}

		// There is already a lock, increment the counter and call trylock()
		keyLockRow = new SimpleEntry<ReentrantLock, Integer>(keyLockRow.getKey(), keyLockRow.getValue() + 1);
		keyLocks.put(key, keyLockRow);
		tableLock.unlock();

		try{
			boolean hasKeyLock = keyLockRow.getKey().tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS);
			if(hasKeyLock){
				return true; // This is the starting time for the process after aquiring the lock
			}
		}
		catch(InterruptedException e){

		} 
		// the timeout occured. decrement the counter and see wether to delete from the hash map
		getTableLock(timeout);
		
		if(keyLockRow.getValue() <= 1){ // number of threads alive (including the caller of this function)
			keyLocks.remove(key);
			tableLock.unlock();
			return false;
		}
		keyLockRow = new SimpleEntry<ReentrantLock, Integer>(keyLockRow.getKey(), keyLockRow.getValue() - 1);
		keyLocks.put(key, keyLockRow);
		tableLock.unlock();
		return false;
	}
	

    @Override
    public void releaseLock(String key) throws LockNotHeldException {

    	// no need to lock for checking that this thread is the owner. if it is then it can aquire the table lock after,
    	SimpleEntry<ReentrantLock, Integer> keyLockRow = keyLocks.get(key);

    	if(keyLockRow == null || !keyLockRow.getKey().isHeldByCurrentThread()){
			throw new LockNotHeldException("Do not have lock " + key);
		}
    	getTableLock(Duration.ofSeconds(2));

		if(keyLocks.get(key).getValue() <= 1){
			keyLockRow.getKey().unlock();
			keyLocks.remove(key);
			tableLock.unlock();
			return;
		}

		keyLocks.get(key).setValue(keyLocks.get(key).getValue() - 1);
		keyLockRow.getKey().unlock();
		tableLock.unlock();
    }

    @Override
    public void flushLocks(){
    	return; // not sure how to implement this and keep it safe if a thread crashes while holding a lock
    }
}