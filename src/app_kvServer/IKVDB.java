package app_kvServer;

public interface IKVDB {


    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the storage of the server
     * Interact with IKVDB (perhaps the KVServer should interact directly with IKVDB)
     */
    public void clearStorage();

    /**
     * Returns an iterable of key value pairs that will be used to load data into the cache.
    */
    public Iterator<String, String> cacheLoader();

}
