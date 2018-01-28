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
    protected LinkedHashMap<String, String> map;
    protected final int capacity;
    

    protected KeyLockManager keyLockManager;

    protected FilePerKeyKVDB kvdb;

    public LFUCache(int capacity) {
        map = new LinkedHashMap<>(capacity);
        this.capacity = capacity;

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
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        try{    
            kvdb.delete(key);
            map.remove(key);
            try{
                keyLockManager.releaseLock(key);
            }catch(KeyLockManager.LockNotHeldException ee){
            }
        }catch(IKVDB.KeyDoesntExistException e){
            try{
                keyLockManager.releaseLock(key);
            }catch(KeyLockManager.LockNotHeldException ee){
            }
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        return;
    }

    @Override
    public synchronized void loadData(Iterator<SimpleEntry<String, String>> iterator) {
        map.clear(); // just in case
        while (iterator.hasNext()) {
            SimpleEntry<String, String> kv = iterator.next();
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

}
