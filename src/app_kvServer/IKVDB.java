package app_kvServer;

import java.util.*;
import java.util.AbstractMap.*;

public interface IKVDB {
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
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Get the value associated with the key
     */
    public String get(String key) throws KeyDoesntExistException;

    /**
     * Put the key-value pair into storage
     * @return true if new tuple inserted, false if tuple updated
     * @throws Exception if the key was not able to be inserted for an undetermined reason
     */
    public boolean put(String key, String value) throws Exception;
    
    /**
     * Delete the key-value pair from storage.
     */
    public void delete(String key) throws KeyDoesntExistException;

    /**
     * Clear the storage of the server
     */
    public void clearStorage();
    
    /**
     * Called to load the data into the db from the cache
    */
    public void loadData(Iterator<Map.Entry<String, String>> iterator) throws Exception;

    /**
     * Returns an Iterator of key value pairs that will be used to load data into the cache.
    */
    public Iterator<Map.Entry<String, String> > iterator();


    /*
     * Only iterate keys so that we don't have to read off disk each time. 
     * Used for hash range iteration.
    */
    public Iterator<String> keyIterator();


}
