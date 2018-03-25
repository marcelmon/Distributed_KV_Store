package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.time.Duration;
import java.io.*;
import java.lang.Exception;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import java.util.Map;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.comms.IConsistentHasher;


public class FIFOCache implements ICache {

    public class FIFOCacheLinkedHashMap extends LinkedHashMap<String, String> {

        private int capacity;
        // private Logger logger;

        public FIFOCacheLinkedHashMap(int capacity) { // access eviction order is true for LRU, false for insertion-order(FIFO)
            super(capacity, 0.75f, false); // 0.75 is loadFactor, true is accessOrder
            this.capacity = capacity;
            // logger = new LogSetup("logs/server/server.log", Level.ALL);
            logger.debug("FIFOCacheLinkedHashMap() " + capacity);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            logger.debug("FIFOCacheLinkedHashMap removeEldestEntry() : " + (this.size() > this.capacity));
            return this.size() > this.capacity;
        }
    }

    
    protected final int capacity;
    
    protected FIFOCacheLinkedHashMap map;
    protected FilePerKeyKVDB kvdb;

    protected static Logger logger = Logger.getRootLogger();

    protected final String data_dir;

    public FIFOCache(int capacity) {
        
        this.capacity = capacity;
        this.data_dir = "data_dir";
        map = new FIFOCacheLinkedHashMap(capacity);
        kvdb = new FilePerKeyKVDB("data_dir");

        logger.debug("FIFOCache() - data_dir : data_dir , capacity : " + capacity );
    }

    public FIFOCache(int capacity, String data_dir) {
        this.data_dir = "data_dir_" + data_dir;
        this.capacity = capacity;

        map = new FIFOCacheLinkedHashMap(capacity);
        kvdb = new FilePerKeyKVDB(this.data_dir);

        logger.debug("FIFOCache() - data_dir : " + this.data_dir + " , capacity : " + capacity );
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
    /**
     * This class doesn't provide persistent storage. 
     */
    public boolean inStorage(String key) {
        if (!validateKey(key)) {
            return false;
            // throw new Exception("Attempted to check in storage for an invalid key");
        }
        return kvdb.inStorage(key);
    }

    @Override
    public synchronized boolean inCache(String key) {
        return map.containsKey(key);
    }

    @Override
    public synchronized String get(String key) throws KeyDoesntExistException {
        if (!validateKey(key)) {
            return null;
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
    public synchronized boolean put(String key, String value) throws Exception {
        if (!validateKey(key)) {
            return false;
        }
        if(map.containsKey(key)){
            if(Objects.equals(map.get(key), value)){
                logger.debug("FIFOCache put() cache hit key and value -> key : " + key + ", value : " + value);
                return false;
            }
            // else the cached value is different
            map.put(key, value);
            kvdb.put(key, value);
            logger.debug("FIFOCache put() cache hit key, update value -> key : " + key + ", value : " + value);
            return false; // tuple updated
        }
        map.put(key, value); // map.put returns null if no previous mapping
        kvdb.put(key, value);
        logger.debug("FIFOCache put() cache miss key : " + key + ", value : " + value);
        return true;
    }

    @Override
    public synchronized void delete(String key) throws KeyDoesntExistException {
        if (!validateKey(key)) {
            return;
        }
        if (!kvdb.inStorage(key)) {
            map.remove(key); // for good measure (but probably don't need due to locking)
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        try {
            if (!kvdb.inStorage(key)){
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
    
    @Override
    public List<Map.Entry<String, String>> getTuples() {   	
    	ArrayList<Map.Entry<String, String>> output = new ArrayList<Map.Entry<String, String>>();
    	for (String k : map.keySet()) {    		
			output.add(new AbstractMap.SimpleEntry<String, String>(k, map.get(k)));
    	}
    	return output;
    }
    
    @Override
    public List<Map.Entry<String, String>> getTuples(Byte[] hashLower, Byte[] hashUpper) {
    	try {
	    	IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();
	    	
	    	ArrayList<Map.Entry<String, String>> output = new ArrayList<Map.Entry<String, String>>();
	    	for (String k : map.keySet()) {    		
	    		// Hash the key:
	    		MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] keyhash = md.digest(k.getBytes());
	    		
	    		if (comp.compare(hashLower, hashUpper) < 0) {  // lower < upper 
	    			// Key hash must be greater than lower *and* less than upper
	    			if (comp.compare(keyhash, hashLower) > 0 && comp.compare(keyhash, hashUpper) < 0) {
	    				output.add(new AbstractMap.SimpleEntry<String, String>(k, map.get(k)));
	    			}
	    		} else {									   // lower >= upper
	    			// Key hash must be greater than lower *or* less than upper
	    			if (comp.compare(keyhash, hashLower) > 0 || comp.compare(keyhash, hashUpper) < 0) {
	    				output.add(new AbstractMap.SimpleEntry<String, String>(k, map.get(k)));
	    			}
	    		}
	    	}
	    	return output;
    	} catch (NoSuchAlgorithmException e) {
    		throw new RuntimeException("MD5 unimplemented");
    	}
    }

}
