package app_kvServer;

import java.util.LinkedHashMap;
import java.util.Map;


import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.time.Duration;
import java.io.*;
import java.lang.Exception;
import java.util.Objects;

import app_kvServer.ILockManager.LockAlreadyHeldException;

public class LFUCache implements ICache {
    protected HashMap<String, String> map;  // the cache
    protected HashMap<String, Integer> usageCounter; // the hit counter on members of cache 
    protected final int capacity;
    protected KeyLockManager keyLockManager;
    protected FilePerKeyKVDB kvdb;

    public LFUCache(int capacity) {
    	this.capacity = capacity;
        map = new HashMap<>(capacity);
        usageCounter = new HashMap<>(capacity);
        keyLockManager = new KeyLockManager();
        kvdb = new FilePerKeyKVDB("./data_dir");
    }
    
    /**
     * Get a value from the cache, and update the counter.
     */
    protected String getFromCache(String key) throws KeyDoesntExistException {
    	if (!map.containsKey(key)) {
    		throw new KeyDoesntExistException("Key not found: " + key);
    	}
    	if (!usageCounter.containsKey(key)) {
    		throw new RuntimeException("Cache and usageCounter out of sync!");
    	}
    	usageCounter.put(key, usageCounter.get(key) + 1);
    	return map.get(key);
    }
    
    /**
     * Evicts the key from the cache, writing through to the persistent storage.
     * @param key
     */
    protected void evict(String key) throws Exception {
    	String value = map.get(key);
    	kvdb.put(key, value);
    	map.remove(key);
    	usageCounter.remove(key);
    }
    
    /**
     * If the key is already in the cache:
     *   * returns false (i.e. update) but takes not other action
     * If the key is in the db:
     *   * (Potentially) evicts LFU key from cache
     *   * Writes key from db through to cache
     *   * Sets hit counter of new key to 0
     * If the key is not in the db:
     *   * (Potentially) evicts LFU key from cache
     *   * Writes an empty key into the cache (value="")
     *   * Sets hit counter of new key to 0
     * @return true if key wasn't in storage (i.e. insert); false if key was in storage (i.e. update)
     */
    protected boolean evictAndReplace(String key) throws KeyDoesntExistException, Exception {
    	if (map.containsKey(key)) {
    		return false; // already in cache so already primed and ready to go; false means update
    	} else {
    		// If above capacity (incl inserted key), evict LFU:
			if (map.size() + 1 >= capacity) {
				// TODO O(n) search for LFU - can easily make more efficient!
    			String lfuKey = null;
    			Integer lfuCnt = null;
    			Iterator<String> it = map.keySet().iterator();
    			while (it.hasNext()) {
    				String k = it.next();
    				Integer c = usageCounter.get(k);
    				if (lfuCnt == null || c < lfuCnt ) {
    					lfuKey = k;
    					lfuCnt = c;
    				}
    			}
    			if (lfuKey != null) {
    				evict(lfuKey);
    			}
			}
    		
    		if (kvdb.inStorage(key)) { 				// storage hit
    			String value = kvdb.get(key);
    			map.put(key, value);
    			usageCounter.put(key, 0);
    			return false; // update
    		} else {								// storage miss
    			// Create placeholder
    			map.put(key, "");
    			usageCounter.put(key, 0);
    			return true; // insert
    		}
    	}
    }
    
    /**
     * Put a value into the cache, update the counter, potentially evict the LFU cache member.
     */
    protected boolean putIntoCache(String key, String value) throws Exception {
    	// Ensure the value is in cache. We want to avoid it being in the storage but not
    	// the cache as this can lead to synchronization errors:
    	boolean inserted = evictAndReplace(key);
    	
    	// Perform an insert:
        map.put(key, value);
    	usageCounter.put(key, usageCounter.get(key) + 1);
    	
    	return inserted;
    }

    @Override
    public int getCacheSize() {
        return capacity;        
    }

    @Override
    public boolean inStorage(String key) {
        return kvdb.inStorage(key);
    }

    @Override
    public synchronized boolean inCache(String key) {
        return map.containsKey(key);
    }

    @Override
    public synchronized String get(String key) throws KeyDoesntExistException, StorageException {   	
        if (map.containsKey(key)) {				// cache hit
        	return getFromCache(key);
        } else {								// cache miss
            if (kvdb.inStorage(key)){			// storage hit
            	try {
            		evictAndReplace(key);
            	} catch (Exception e) {
            		throw new StorageException(e.getMessage());
            	}
            	return getFromCache(key);
            } else {							// storage miss
            	throw new KeyDoesntExistException("Key \"" + key + "\" doesn't exist");
            }
        }
    }

    /**
     * Put the key-value pair into storage
     * @return true if new tuple inserted, false if tuple updated
     * @throws Exception if the key was not able to be inserted for an undetermined reason
     */
    @Override
    public synchronized boolean put(String key, String value) throws Exception {
        boolean gotLock = keyLockManager.getLock(key, Duration.ofSeconds(5));
        if(!gotLock){
            throw new Exception("Failed to get lock on key=" + key);
        }
        // we now have a lock on the key
        
        boolean inserted = this.putIntoCache(key, value);
        
        // We must remember to release the lock
        keyLockManager.releaseLock(key);
        
        return inserted;
    }

    @Override
    public synchronized void delete(String key) throws KeyDoesntExistException {
        boolean gotLock;
        try{
            gotLock = keyLockManager.getLock(key, Duration.ofSeconds(5));
        } catch(LockAlreadyHeldException e){
            gotLock = true;
        }
        if(!gotLock){
        	throw new RuntimeException("Failed to acquire a lock!");
        }
        
        // Remove the key from the database and storage. If it doesn't exist in either, throw an exception.
        // Notably, it is possible for it to exist in just the cache, just the db, or in both.
        boolean existed = false;
        if (map.containsKey(key)) {
        	map.remove(key);
        	existed = true;
        }
        if (kvdb.inStorage(key)) {
        	try {
        		kvdb.delete(key);        		
        		existed = true;
        	} catch (IKVDB.KeyDoesntExistException e) {
        		throw new RuntimeException("Fatal error: key doesnt exist after db reports it does");
        	}
        }
        if (!existed) {
        	throw new KeyDoesntExistException("Key doesnt exist in cache or db: " + key);
        }
        
        // Always release the lock:
        try {
            keyLockManager.releaseLock(key);
        } catch(KeyLockManager.LockNotHeldException ee){
        	throw new RuntimeException("Completely unexpected! Lock not held");
        }
        
    }

    @Override
    public synchronized void loadData(Iterator<SimpleEntry<String, String>> iterator) {
        map.clear(); // just in case
        usageCounter.clear();
        while (iterator.hasNext()) {
            SimpleEntry<String, String> kv = iterator.next();
            map.put(kv.getKey(), kv.getValue());
            usageCounter.put(kv.getKey(), 0);
        }
    }

    @Override
    public synchronized void clearCache() {
        map.clear();
    }

    @Override
    public synchronized void clearPersistentStorage() {
        kvdb.clearStorage();
    }

}
