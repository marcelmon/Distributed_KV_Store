package app_kvServer;

import java.util.*;
import java.util.AbstractMap.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class IKVDBDecorator implements IKVDB{

	protected IKVDB kvdb;

    protected static Logger logger = Logger.getRootLogger();

    public IKVDBDecorator(IKVDB kvdb) {
        this.kvdb = kvdb;
        logger.debug("IKVDBDecorator:" + kvdb.getClass());
    }
    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key) {
        boolean result = kvdb.inStorage(key);
        logger.debug(kvdb.getClass() + ", inStorage()  key : " + key +" -> result: " + result);
        return result;
    }

    /**
     * Get the value associated with the key
     */
    public String get(String key) throws KeyDoesntExistException {
        String result = kvdb.get(key);
        logger.debug(kvdb.getClass() + ", get()  key : " + key +" -> result: " + result);
        return result;
    }

    /**
     * Put the key-value pair into storage
     * @return true if new tuple inserted, false if tuple updated
     * @throws Exception if the key was not able to be inserted for an undetermined reason
     */
    public boolean put(String key, String value) throws Exception {
        boolean result = kvdb.put(key, value);
        logger.debug(kvdb.getClass() + ", put()  key : " + key +", value : " + value + " -> result: " + result);
        return result;
    }
    
    /**
     * Delete the key-value pair from storage.
     */
    public void delete(String key) throws KeyDoesntExistException {
        kvdb.delete(key);
        logger.debug(kvdb.getClass() + ", delete()  key : " + key);
    }

    /**
     * Clear the storage of the server
     */
    public void clearStorage() {
        kvdb.clearStorage();
        logger.debug(kvdb.getClass() + ", clearStorage()");
    }
    
    /**
     * Called to load the data into the db from the kvdb
    */
    public void loadData(Iterator<Map.Entry<String, String>> iterator) throws Exception {
        kvdb.loadData(iterator);
        logger.debug(kvdb.getClass() + ", loadData()");
    }

    /**
     * Returns an Iterator of key value pairs that will be used to load data into the cache.
    */
    public Iterator<Map.Entry<String, String> > iterator() {
        logger.debug(kvdb.getClass() + ", iterator()");
        return kvdb.iterator();
    }

    public Iterator<String> keyIterator() {
        logger.debug(kvdb.getClass() + ", keyIterator()");
        return kvdb.keyIterator();
    }

}
