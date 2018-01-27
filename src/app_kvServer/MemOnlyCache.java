package app_kvServer;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.HashMap;

public class MemOnlyCache implements ICache {
	protected HashMap<String, String> map;
	protected final int capacity;
	
	public MemOnlyCache(int capacity) {
		map = new HashMap<>(capacity);
		this.capacity = capacity;
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
		return false;
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
		return map.put(key, value) == null; // map.put returns null if no previous mapping
	}

	@Override
	public synchronized void delete(String key) throws KeyDoesntExistException {
		if (map.remove(key) == null) {
			throw new KeyDoesntExistException("Attempted to delete key \"" + key + "\" which doesn't exist");
		}
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
		// no persistent storage => do nothing
	}

}
