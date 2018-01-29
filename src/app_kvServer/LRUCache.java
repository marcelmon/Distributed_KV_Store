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

import app_kvServer.ILockManager.LockAlreadyHeldException;

public class LRUCache implements ICache {


    public class LRUCacheLinkedHashMap extends LinkedHashMap<String, String> {

        private int capacity;

        public LRUCacheLinkedHashMap(int capacity) { // access order is true for LRU, false for insertion-order(FIFO)
            this.capacity = capacity;
            super(capacity, 0.75f, true); // 0.75 is loadFactor, true is accessOrder
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return this.size() > this.capacity;
        }
    }

    
    protected final int capacity;
    
    protected LRUCacheLinkedHashMap map;
    protected KeyLockManager keyLockManager;
    protected FilePerKeyKVDB kvdb;

    public LRUCache(int capacity) {
        
        this.capacity = capacity;

        map = new LRUCacheLinkedHashMap(capacity);
        keyLockManager = new KeyLockManager();
        kvdb = new FilePerKeyKVDB("./data_dir");
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
        boolean gotLock = keyLockManager.getLock(key, Duration.ofSeconds(5));
        if(!gotLock){
            throw new Exception("Key " + key + " not updated!");
        }
        if(map.containsKey(key)){
            if(Objects.equals(map.get(key), value)){
                // no update
                return false;
            }
            // else the cached value is different
            map.put(key, value);
            kvdb.put(key, value);
            keyLockManager.releaseLock(key);
            return false; // tuple updated
        }
        map.put(key, value); // map.put returns null if no previous mapping
        kvdb.put(key, value);
        keyLockManager.releaseLock(key);
        return true;
    }

    @Override
    public synchronized void delete(String key) throws KeyDoesntExistException {
        boolean gotLock;
        try{
            gotLock = keyLockManager.getLock(key, Duration.ofSeconds(5));
        }catch(LockAlreadyHeldException e){
            gotLock = true;
        }
        if(!gotLock){
            return;
        }
        if(!kvdb.inStorage(key)){
            map.remove(key); // for good measure (but probably don't need due to locking)
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        try{
            if(!kvdb.inStorage(key)){
                throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
            }
            kvdb.delete(key);
            map.remove(key);
        }catch(IKVDB.KeyDoesntExistException e){
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


}