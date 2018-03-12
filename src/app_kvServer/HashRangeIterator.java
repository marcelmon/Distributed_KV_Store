package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;
import java.time.Duration;
import java.io.*;
import java.lang.Exception;
import java.util.Objects;

import java.util.Map;

import java.util.NoSuchElementException;


import java.security.MessageDigest;


import java.util.Comparator;

class HashRangeIterator implements Iterator<Map.Entry<String, String>> {

    private boolean hasCache = false;
    private boolean hasKVDB = false;

    private Iterator<String> kvdbIterator;
    private Iterator<Map.Entry<String, String>> cacheIterator;

    private HashMap<String, String> foundCacheKeys;

    private boolean hasClearedCache = false;

    private byte[] counterClockwiseBound;
    private byte[] clockwiseBound;

    // cannot rewind iterator in so when fast forwarding in next() we will save the key
    private boolean hasPendingKeyValue = false;
    private String pendingKey;
    private String pendingValue;


    private IKVDB kvdb;

    private boolean multipleRanges = false;


    public class HashByteComparator implements Comparator<byte[]> {
        @Override
        public int compare(byte[] arg0, byte[] arg1) {
            if (arg0.length < arg1.length) {
                return -1;
            } else if (arg0.length > arg1.length) {
                return 1;
            }
            
            // same length
            for (int i = 0; i < arg0.length; i++) {
                if (arg0[i] < arg1[i]) {
                    return -1;
                } else if (arg0[i] > arg1[i]) {
                    return 1;
                }
            }
            return 0;
        }       
    }


    public HashRangeIterator(byte[] counterClockwiseBound, byte[] clockwiseBound, ICache cache, IKVDB kvdb) {

        if(cache != null){
            hasCache = true;
            cacheIterator  = cache.iterator(); 
        }
        if(kvdb != null){
            hasKVDB = true;
            kvdbIterator = kvdb.keyIterator();
            this.kvdb = kvdb;
        }
        
        foundCacheKeys = new HashMap<String, String>();

        this.counterClockwiseBound = counterClockwiseBound;
        this.clockwiseBound = clockwiseBound;
    }

    public boolean isBetween(byte[] fileNameHash) {

        HashByteComparator comp = new HashByteComparator();

        if(comp.compare(counterClockwiseBound, clockwiseBound) > 0){

            // PASSES THROUGH the center
            // Check if greater than counterClockwiseBound bound 
            // OR
            // If is less than clockwise bound
            if(comp.compare(fileNameHash, clockwiseBound) < 0  || comp.compare(fileNameHash, counterClockwiseBound) > 0){
                return true;
            }
        }
        // does not pass over center, need to be less than clockwise AND greater than coutner clockwise
        else if(comp.compare(fileNameHash, clockwiseBound) < 0  && comp.compare(fileNameHash, counterClockwiseBound) > 0){
            return true;
        }
        return false;
    }

    public boolean keyInRange(String key) {
        byte[] fileNameHash = null;
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");
            fileNameHash = md.digest(key.getBytes());
        } 
        catch(Exception e) {
            throw new RuntimeException(e.getMessage());
        }

        if(isBetween(fileNameHash)){
            return true;
        }
        return false;
    }


    public boolean fastForwardCacheIterator() {
        if(hasCache == false){
            return false;
        }
        int i = 0;

        while(cacheIterator.hasNext()){
            Map.Entry<String, String> cachedKeyValue = cacheIterator.next();
            if(keyInRange(cachedKeyValue.getKey()) == true){
                hasPendingKeyValue = true;
                pendingKey = cachedKeyValue.getKey();
                pendingValue = cachedKeyValue.getValue();

                foundCacheKeys.put(pendingKey, pendingValue); // add to found keys so as not to read from disk later
                return true;
            }

        }
        return false;
    }


    public boolean fastForwardKVDBIterator() {
        if(hasKVDB == false){
            return false;
        }
        while(kvdbIterator.hasNext()){

            String dbKey = kvdbIterator.next();
            if(keyInRange(dbKey) == true && !foundCacheKeys.containsKey(dbKey)){
                hasPendingKeyValue = true;
                pendingKey = dbKey;
                try{
                    pendingValue = kvdb.get(dbKey); // pre-load the value 
                } 
                catch(Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
                
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean hasNext() {
        if(hasPendingKeyValue == true){
            return true;
        }
        if(!hasClearedCache){
            if(fastForwardCacheIterator()){
                return true;
            }
            hasClearedCache = true;
        }
        if(fastForwardKVDBIterator()){
            return true;
        }

        return false;
    }

    @Override
    public Map.Entry<String, String> next() throws NoSuchElementException {
        if(!hasNext()){
            hasPendingKeyValue = false;
            throw new NoSuchElementException();
        }
        hasPendingKeyValue = false;
        return new SimpleEntry<String, String>(pendingKey, pendingValue);
    }

}