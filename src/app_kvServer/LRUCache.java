package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.time.Duration;
import java.io.*;
import java.lang.Exception;
import java.util.Objects;

import java.util.Map;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class LRUCache implements ICache {
    public class LRUCacheLinkedHashMap extends LinkedHashMap<String, String> {

        private int capacity;

        public LRUCacheLinkedHashMap(int capacity) { // access order is true for LRU, false for insertion-order(FIFO)
            super(capacity, 0.75f, true); // 0.75 is loadFactor, true is accessOrder
            this.capacity = capacity;
            logger.debug("LRUCacheLinkedHashMap(), capacity : " + capacity);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            boolean result = this.size() > this.capacity;
            logger.debug("LRUCacheLinkedHashMap removeEldestEntry() -> result : " + result);
            return result;
        }
    }

    protected static Logger logger = Logger.getRootLogger();
    
    protected final int capacity;
    
    protected LRUCacheLinkedHashMap map;
    protected FilePerKeyKVDB kvdb;

    protected final String data_dir;

    public LRUCache(int capacity) {
        this.capacity = capacity;

        map = new LRUCacheLinkedHashMap(capacity);
        kvdb = new FilePerKeyKVDB("data_dir");

        data_dir = "data_dir";

        logger.debug("LRUCache() - data_dir : data_dir, capacity : " + capacity );
    }

    public LRUCache(int capacity, String data_dir) {
        this.capacity = capacity;
        this.data_dir = "data_dir_" + data_dir;
        map = new LRUCacheLinkedHashMap(capacity);
        kvdb = new FilePerKeyKVDB(this.data_dir);


        logger.debug("LRUCache() - data_dir : " + this.data_dir + ", capacity : " + capacity );
    }
    
    @Override
	public boolean validateKey(String key) {
		boolean result = !key.isEmpty() && !key.contains(" ") && !(key.length() > 20);
		// System.out.println("Key [" + key + "] validates: " + result);
		return result;
	}

    @Override
    public int getCacheSize() {
        return capacity;        
    }

    @Override
    public boolean inStorage(String key) {
        if (!validateKey(key)) {
            return false; 
        }
        return kvdb.inStorage(key);
    }

    @Override
    public synchronized boolean inCache(String key) {
        return map.containsKey(key);
    }

    @Override
    public synchronized String get(String key) throws KeyDoesntExistException, Exception {
	    if (!validateKey(key)) {
		    throw new Exception("Invalid key");
	    }
        if (map.containsKey(key)) {
            logger.debug("get() cache hit");
            return map.get(key);            
        } else {
            if(kvdb.inStorage(key)){
                logger.debug("get() stored key found");
                try{
                    String value = kvdb.get(key);
                    map.put(key, value);
                    return value;
                }catch(IKVDB.KeyDoesntExistException e){
                    throw new KeyDoesntExistException("Key \"" + key + "\" doesn't exist");
                }
            }
            throw new KeyDoesntExistException("Key \"" + key + "\" doesn't exist");
        }
    }

    /**
     * Put the key-value pair into storage
     * @return true if new tuple inserted, false if tuple updated
     * @throws Exception if the key was not able to be inserted for an undetermined reason
     */
    @Override
    public synchronized boolean put(String key, String value) throws Exception, Exception {
		if (!validateKey(key)) {
			throw new Exception("Attempted to put an empty key");
		}
        if(map.containsKey(key)){
            if(Objects.equals(map.get(key), value)){
                logger.debug("put() cached key value");
                // no update
                return false;
            }
            // else the cached value is different
            logger.debug("put() cached key update value");
            map.put(key, value);
            kvdb.put(key, value);
            return false; // tuple updated
        }
        logger.debug("put() cached miss key");
        map.put(key, value); // map.put returns null if no previous mapping
        kvdb.put(key, value);
        return true;
    }

    @Override
    public synchronized void delete(String key) throws KeyDoesntExistException {
        if(!kvdb.inStorage(key)){
            map.remove(key);
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        try {
            if(!kvdb.inStorage(key)){
                throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
            }
            kvdb.delete(key);
            map.remove(key);
        } catch(IKVDB.KeyDoesntExistException e) {
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        return;
    }

    @Override
    public synchronized void loadData(Iterator<Map.Entry<String, String>> iterator) {
        map.clear(); // just in case
        while (iterator.hasNext()) {
            Map.Entry<String, String> kv = iterator.next();
            map.put(kv.getKey(), kv.getValue());
        }
    }

    @Override
    public synchronized void clearCache() {
        map.clear();
    }

    @Override
    public synchronized void clearPersistentStorage() {
        clearCache();
    	kvdb.clearStorage();
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void writeThrough() throws Exception {
        kvdb.loadData(iterator());
    }


    @Override 
    public Iterator<Map.Entry<String, String>> getHashRangeIterator(byte[] minHash, byte[] maxHash) {
        return new HashRangeIterator(minHash, maxHash, this, kvdb);
    }
    
}
