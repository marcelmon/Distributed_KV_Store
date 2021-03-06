package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class NoCache implements ICache {
	
    protected FilePerKeyKVDB kvdb;
    protected final String data_dir;
    protected static Logger logger = Logger.getRootLogger();
    
	public boolean validateKey(String key) {
		boolean result = !key.isEmpty() && !key.contains(" ") && !(key.length() > 20);
		// System.out.println("Key [" + key + "] validates: " + result);
		return result;
	}


	public NoCache() {

		logger.debug("NoCache() - data_dir : data_dir");
		kvdb = new FilePerKeyKVDB("data_dir");
		this.data_dir = "data_dir";
	}

	public NoCache(String data_dir) {
        
		this.data_dir = "data_dir_" + data_dir;

        logger.debug("NoCache() - data_dir : " + this.data_dir);

        kvdb = new FilePerKeyKVDB(this.data_dir);

    }

	@Override
	public int getCacheSize() {
		return 0;		
	}

	@Override
	public boolean inStorage(String key) {
		if (!validateKey(key)) {
            // throw new Exception("Attempted to check in storage for an invalid key");
	    }
		return kvdb.inStorage(key);
	}

	@Override
	public boolean inCache(String key) {
		return false;
	}

	@Override
	public String get(String key) throws KeyDoesntExistException {
		if (!validateKey(key)) {
            // throw new Exception("Attempted to get an invalid key");
	    }
		if(!kvdb.inStorage(key)){
			throw new KeyDoesntExistException("Key \"" + key + "\" doesn't exist");
		}
		try{
			String returnVal = kvdb.get(key); 
			return returnVal;
		}catch(IKVDB.KeyDoesntExistException e){
		}
		return null;
	}

	/**
     * Put the key-value pair into storage
     * @return true if new tuple inserted, false if tuple updated
     * @throws Exception if the key was not able to be inserted for an undetermined reason
     */
	@Override
	public synchronized boolean put(String key, String value) throws Exception {
		if (!validateKey(key)) {
            // throw new Exception("Attempted to put an invalid key");
	    }
        try{
        	boolean a = kvdb.put(key, value);
        	return a;
        }catch(Exception e){
        	throw new Exception("exception.!");
        }
	}

	@Override
	public synchronized void delete(String key) throws KeyDoesntExistException {
		if (!validateKey(key)) {
            // throw new Exception("Attempted to delete an invalid key");
	    }
        if(!kvdb.inStorage(key)){
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
		try{
            kvdb.delete(key);
        }catch(IKVDB.KeyDoesntExistException e){
            throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
        }
        return;
	}

	@Override
	/* Nothing to do */
	public synchronized void loadData(Iterator<Map.Entry<String, String>> iterator) {
		return;
	}

	@Override
	/* Nothing to do */
	public synchronized void clearCache() {
		return;
	}

	@Override
	public synchronized void clearPersistentStorage() {
		kvdb.clearStorage();
		return;
	}

	@Override
	public void writeThrough() {
		// do nothing because there is no cache
	}


	@Override
	public Iterator<Entry<String, String>> iterator() {
		return null;
		// return kvdb.iterator(); // no cache to iterate over (could provide the lower level iterator)
	}

	@Override 
	public Iterator<Map.Entry<String, String>> getHashRangeIterator(byte[] minHash, byte[] maxHash) {
		return new HashRangeIterator(minHash, maxHash, null, kvdb);
	}
}
