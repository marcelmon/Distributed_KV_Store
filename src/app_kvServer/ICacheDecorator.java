package app_kvServer;

import java.util.Iterator;
import java.util.Map;
import java.util.AbstractMap.*;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ICacheDecorator implements ICache {


	
	protected ICache cache;

	protected static Logger logger = Logger.getRootLogger();

	ICacheDecorator(ICache cache) {
        this.cache = cache;
        logger.debug("ICacheDecorator:" + cache.getClass());
    }

	/**
	 * Returns true if the given key is valid (is non-empty, <= 20 bytes long and without whitespace)
	 * @param key
	 * @return
	 */
	@Override
	public boolean validateKey(String key) {
		boolean result = cache.validateKey(key);
		logger.debug(cache.getClass() + ", validateKey() -> result: " + result);
		return result;
	}

	/**
	 * Get the (maximum) cache size
	 * 
	 * @return cache size
	 */
	public int getCacheSize(){
		int cacheSize = cache.getCacheSize();
		logger.debug(cache.getClass() + ", getCacheSize() -> cacheSize: " + cacheSize);
		return cacheSize;
	}

	/**
	 * Check if key is in storage. NOTE: does not modify any other properties
	 * 
	 * @return true if key in storage, false otherwise Perhaps the KVServer should
	 *         interact directly with the IKVDB
	 */
	public boolean inStorage(String key){
		boolean result = cache.inStorage(key);
		logger.debug(cache.getClass() + ", inStorage()  key : " + key +" -> result: " + result);
		return result;
	}

	/**
	 * Check if key is in storage. NOTE: does not modify any other properties
	 * 
	 * @return true if key in storage, false otherwise
	 */
	public boolean inCache(String key){
		boolean result = cache.inCache(key);
		logger.debug(cache.getClass() + ", inCache()  key : " + key +" -> result: " + result);
		return result;
	}

	/**
	 * Get the value associated with the key
	 */
	public String get(String key) throws KeyDoesntExistException, StorageException, Exception {
		String result = cache.get(key);
		logger.debug(cache.getClass() + ", get()  key : " + key +" -> result: " + result);
		return result;
	}

	/**
	 * Insert/update the key-value pair.
	 * 
	 * @return true if new tuple inserted, false if tuple updated
	 * @throws Exception
	 *             if the key was not able to be inserted for an undetermined reason
	 */
	public boolean put(String key, String value) throws Exception {
		boolean result = cache.put(key, value);
		logger.debug(cache.getClass() + ", put()  key : " + key +", value : " + value + " -> result: " + result);
		return result;
	}

	/**
	 * Delete the key-value pair.
	 */
	public void delete(String key) throws KeyDoesntExistException, Exception {
		cache.delete(key);
		logger.debug(cache.getClass() + ", delete()  key : " + key);
	}

	/**
	 * Called to load the data into the cache from IKVDB.iterator()
	 */
	public void loadData(Iterator<Map.Entry<String, String>> iterator) {
		cache.loadData(iterator);
		logger.debug(cache.getClass() + ", loadData()");
	}

	/**
	 * Returns an Iterator of key value pairs that will be used to load data into
	 * the storage.
	 */
	public Iterator<Map.Entry<String, String>> iterator() {
		logger.debug(cache.getClass() + ", iterator()");
		return cache.iterator();
	}

	/**
	 * Clear the local cache of the server
	 */
	public void clearCache() {
		cache.clearCache();
		logger.debug(cache.getClass() + ", clearCache()");
	}

	/**
	 * Clear the storage of the server
	 */
	public void clearPersistentStorage() {
		cache.clearPersistentStorage();
		logger.debug(cache.getClass() + ", clearPersistentStorage()");
	}

	/**
	 * Ensure the storage is up to date with the cache.
	 */
	public void writeThrough() throws Exception {
		cache.writeThrough();
		logger.debug(cache.getClass() + ", writeThrough()");
	}


	public Iterator<Map.Entry<String, String>> getHashRangeIterator(byte[] minHash, byte[] maxHash) {
		logger.debug(cache.getClass() + ", getHashRangeIterator()");
		return cache.getHashRangeIterator(minHash, maxHash);
	}

}
