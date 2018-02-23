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

public class FIFOCache implements ICache {

    public class FIFOCacheLinkedHashMap extends LinkedHashMap<String, String> {

        private int capacity;

        public FIFOCacheLinkedHashMap(int capacity) { // access eviction order is true for LRU, false for insertion-order(FIFO)
            super(capacity, 0.75f, false); // 0.75 is loadFactor, true is accessOrder
            this.capacity = capacity;
            
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() > this.capacity;
        }
    }

    
    protected final int capacity;
    
    protected FIFOCacheLinkedHashMap map;
    protected FilePerKeyKVDB kvdb;

    public FIFOCache(int capacity) {
        
        this.capacity = capacity;

        map = new FIFOCacheLinkedHashMap(capacity);
        kvdb = new FilePerKeyKVDB("data_dir");

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
        return kvdb.inStorage(key);
    }

    @Override
    public synchronized boolean inCache(String key) {
        return map.containsKey(key);
    }

    @Override
    public synchronized String get(String key) throws KeyDoesntExistException {
        if (map.containsKey(key)) {
            return map.get(key);            
        } else {
            if(kvdb.inStorage(key)){
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
        if(map.containsKey(key)){
            if(Objects.equals(map.get(key), value)){
                // no update
                return false;
            }
            // else the cached value is different
            map.put(key, value);
            kvdb.put(key, value);
            return false; // tuple updated
        }
        map.put(key, value); // map.put returns null if no previous mapping
        kvdb.put(key, value);
        return true;
    }

    @Override
    public synchronized void delete(String key) throws KeyDoesntExistException {
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

}
