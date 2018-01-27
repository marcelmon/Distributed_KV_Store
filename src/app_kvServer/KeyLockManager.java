

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


public class SynchronizedLockManager implements ILockManager {

	// HashMap<Key, SimpleEntry<KeyLock, LinkedList<ThreadIds in queue>> // maybe have some time info for forced timeouts/ garbage collection
	protected HashMap<String, SimpleEntry<ReentrantLock, LinkedList<Long>>> keyLocks;


	protected ReentrantLock tableLock = new ReentrantLock();

	private int activeKeyLocks;


	public SynchronizedLockManager() {
		
		activeKeyLocks = 0;
		
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
	 * see if there is no one else locking, otherwise add the thread id to the queue and trylock() with timeout.
	 * 
	 * Unlock the table lock : either after creating a new key lock (and locking it)
	 *						 or when found out that this thread already has the key lock
	 *						Or after adding the thread id to the queue.
	 *
	 *	If the thread id was added to the queue then it will be popped off when the lock is aquired.
	 */

	@Override
	public boolean getLock(String key, Duration timeout) throws LockAlreadyHeldException {
		Long startTimestampMilis = new Date().getTime();
		if(!getTableLock(timeout)){
			return false; // timeout reached but this shouldn't happen
		}
		else{

			SimpleEntry<ReentrantLock, LinkedList<Long>> keyLockRow = keyLocks.get(key);
			if(keyLockRow == null){
				// want to put a new key lock in the table, get that lock, and release the table lock, also initialize the linked list for subsequent threads requesting the key lock
				
				ReentrantLock keyLock = new ReentrantLock();
				keyLock.lock();
				LinkedList<Long> threadIdQueue = new LinkedList<Long>();

				keyLockRow = new SimpleEntry<ReentrantLock, LinkedList<Long>>(keyLock, threadIdQueue);
				

				keyLocks.put(key, keyLockRow);
				activeKeyLocks++;
				tableLock.unlock();
				return true;
			}

			// keyLockRow is Simple Entry, the key is the lock, the value is the linked list queue
			if(keyLockRow.getKey().isHeldByCurrentThread()){
				tableLock.unlock();
				throw new LockAlreadyHeldException("Already have lock " + key);
			}

			// do this so that when the key lock is released it will not remove the HashMap entry
			// the releasing thread will have to wait for table lock to be released
			keyLockRow.getValue().add(Thread.currentThread().getId());

			tableLock.unlock();

			try{
				// aquire the key lock and move the thread id from the queue
				boolean hasKeyLock = keyLockRow.getKey().tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS);
				if(hasKeyLock){
					keyLockRow.getValue().remove(Thread.currentThread().getId());
					return true;
				}
			}
			catch(InterruptedException e){

			}
			
			keyLockRow.getValue().remove(Thread.currentThread().getId());
			return false;
		}
	}

    @Override
    public void releaseLock(String key) throws LockNotHeldException {

    	// no need to lock for checking that this thread is the owner. if it is then it can aquire the table lock after,
    	ReentrantLock keyLock = keyLocks.get(key).getKey();
    	if(keyLock == null || !keyLock.isHeldByCurrentThread()){
			throw new LockNotHeldException("Do not have lock " + key);
		}
    	if(!getTableLock(Duration.ofSeconds(2))){
			// return false; // timeout reached but this shouldn't happen
		}

		if(keyLocks.get(key).getValue().size() == 0){
			// can remove this entry from keyLocks entirely
			keyLocks.get(key).getKey().unlock();
			keyLocks.remove(key);
			activeKeyLocks--;
		}
		else{
			// when the lock is unlocked, some other waiting process will now have it
			keyLocks.get(key).getKey().unlock();
		}
		tableLock.unlock();
    }

    @Override
    public void flushLocks(){
    	return; // not sure how to implement this and keep it safe if a thread crashes while holding a lock
    }
}