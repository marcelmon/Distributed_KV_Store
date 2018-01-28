package app_kvServer;

import java.util.Iterator;
import java.util.AbstractMap.*;

public interface ICache {
	/**
	 * Thrown if a key is expected to exist in the db but doesn't.
	 */
	public class KeyDoesntExistException extends Exception {
		private static final long serialVersionUID = 1L;
		public KeyDoesntExistException(String msg) {
			super(msg);
		}
	}
	
	/**
	 * Thrown if a critical exception occurs with storage e.g. an inability to write.
	 */
	public class StorageException extends Exception {
		private static final long serialVersionUID = 1L;
		public StorageException(String msg) {
			super(msg);
		}
	}

    /**
     * Get the (maximum) cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     * Perhaps the KVServer should interact directly with the IKVDB
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     */
    public String get(String key) throws KeyDoesntExistException, StorageException;

    /**
     * Insert/update the key-value pair.
     * @return true if new tuple inserted, false if tuple updated
     * @throws Exception if the key was not able to be inserted for an undetermined reason
     */
    public boolean put(String key, String value) throws Exception;
    
    /**
     * Delete the key-value pair.
     */
    public void delete(String key) throws KeyDoesntExistException;

    /**
     * Called to load the data into the cache from IKVDB.iterator()
    */
    public void loadData(Iterator<SimpleEntry<String, String>> iterator);

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearPersistentStorage();

}
