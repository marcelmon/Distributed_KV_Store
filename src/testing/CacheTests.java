package testing;

import java.util.Iterator;
import java.util.Map;
import java.lang.reflect.Constructor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;
import common.messages.KVMessage.StatusType;

public class CacheTests extends TestCase {
	protected ICache[] caches;
	protected int desiredCapacity;
	
	@Override
	public void setUp() {
		desiredCapacity = 10;
		caches = new ICache[3];
		caches[0] = new MemOnlyCache(desiredCapacity);
		caches[1] = new LFUCache(desiredCapacity);
		caches[1].clearPersistentStorage();

		caches[2] = new LRUCache(desiredCapacity);
		caches[2].clearPersistentStorage();

		caches[2] = new FIFOCache(desiredCapacity);
		caches[2].clearPersistentStorage();
	}
	
	@Test
	public void testInsert() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.put(keys[i], values[i]));  // true for new
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]) == values[i]);
			}
		}
	}
	
	@Test
	public void testUpdate() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:

			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.put(keys[i], values[i]));  // true for new
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]).equals(values[i]));
			}
			
			// Update:
			assertTrue(cache.get("b").equals("2"));
			assertFalse(cache.put("b", "72")); // false for update
			assertTrue(cache.get("b").equals("72"));
		}
	}
	
	@Test
	public void testDelete() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				cache.put(keys[i], values[i]);
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]) == values[i]);
			}
			
			// Delete:
			cache.delete("b");
			
			// Check not present:
			assertFalse(cache.inCache("b"));
		}
	}
	
	@Test
	public void testInvalidGet() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.put(keys[i], values[i]));  // true for new
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]).equals(values[i]));
			}
			
			// Invalid get:
			boolean caught = false;
			try {
				cache.get("d");
			} catch (ICache.KeyDoesntExistException e) {
				caught = true;
			}
			assertTrue(caught);
		}
	}
	
	@Test
	public void testInvalidDelete() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.put(keys[i], values[i]));  // true for new
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]).equals(values[i]));
			}
			
			// Invalid delete:
			boolean caught = false;
			try {
				cache.delete("d");
			} catch (ICache.KeyDoesntExistException e) {
				caught = true;
			}
			assertTrue(caught);
		}
	}
	
	@Test
	public void testCapacity() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.put(keys[i], values[i]));  // true for new
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]).equals(values[i]));
			}
			
			// Cache size should be max size (capacity), not current size:
			assertTrue(cache.getCacheSize() == 10);
		}
	}
	
	@Test
	public void testClearCache() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		
		for (ICache cache : caches) {
			// Check not present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));	
			}
			
			// Insert
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.put(keys[i], values[i]));  // true for new
			}
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				assertTrue(cache.get(keys[i]).equals(values[i]));
			}
			
			// Clear the cache:
			cache.clearCache();
			
			// Check the keys are no longer present:
			for (int i = 0; i < keys.length; i++) {
				assertFalse(cache.inCache(keys[i]));
			}
		}
	}
	
	@Test
	public void testLoadCache() throws Exception {
		// Contents to load:
		ArrayList<Map.Entry<String, String>> list = new ArrayList<>();
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		assertTrue(keys.length == values.length);
		for (int i = 0; i < keys.length; i++) {
			list.add(new SimpleEntry<String, String>(keys[i], values[i]));
		}
		
		for (ICache cache : caches) {
			// Put a key in to start - this should be destroyed:
			try {
				cache.put("erroneous", "1");
			} catch (Exception e) {
				fail("Failed to insert a key");
			}
			
			// Load from iterator:
			cache.loadData(list.iterator());
			
			// Check present:
			for (int i = 0; i < keys.length; i++) {
				assertTrue(cache.inCache(keys[i]));
				try {
					assertTrue(cache.get(keys[i]).equals(values[i]));
				} catch (ICache.KeyDoesntExistException e) {
					fail("Fatal error: ICache.get() reports presence but KeyDoesntExistException thrown!");
				}
			}
			
			// Check error key not present:
			assertFalse(cache.inCache("erroneous"));
		}
	}
	
	@Test
	public void testEviction() throws Exception {
		final int N = 10;
		
		for (ICache cache : caches) {
			if (cache.getClass().equals(MemOnlyCache.class)) {
//				System.out.println("Skipping " + MemOnlyCache.class.getSimpleName());
				// Skip MemOnlyCache because it doesn't maintain capacity
				break;
			}
			assertTrue(cache.getCacheSize() == N); // this test assumes capacity is 10
			
			// Insert N keys:
			for (int i = 0; i < N; i++) {
				cache.put(Integer.toString(i), Integer.toString(i+100));
			}
			
			// Read all but the 2nd twice:
			for (int i = 0; i < N; i++) {
				cache.get(Integer.toString(i));
				if (i != 1) {
					cache.get(Integer.toString(i));	
				}
			}
			
			// Check all keys are present:
			for (int i = 0; i < N; i++) {
				assertTrue(cache.inCache(Integer.toString(i)));	
			}
			
			// Insert another key. This should cause the cache to evict the second key:
			assertTrue(cache.put(Integer.toString(N), Integer.toString(N+100))); // true for insert
			
			// Check all keys are present except for the 2nd (key=1):
			for (int i = 0; i < N+1; i++) {
				if (i == 1) {
					assertFalse(cache.inCache(Integer.toString(i)));
				} else {
					assertTrue(cache.inCache(Integer.toString(i)));
				}
			}
		}
	}
	
	@Test
	public void testInvalidKeys() throws Exception {
		for (ICache cache : caches ) {
			String key0 = "abcdefg";
			assertTrue(cache.validateKey(key0));
			
			String key1 = "abc ";  // white space
			assertFalse(cache.validateKey(key1));
			
			String key2 = "ab c"; // white space
			assertFalse(cache.validateKey(key2));
			
			String key3 = ""; // empty
			assertFalse(cache.validateKey(key3));
			
			String key4 = "12345678901234567890"; // 20 characters is okay
			assertTrue(cache.validateKey(key4));
			
			String key5 = "123456789012345678901";  // 21 characters is not
			assertFalse(cache.validateKey(key5));
		}
	}
	
	@Test
	public void testClearPersistent() throws Exception {
		for (ICache cache : caches) {
			if (cache.getClass().equals(MemOnlyCache.class)) {
				// Skip MemOnlyCache because it doesn't maintain capacity
				break;
			}
			
			// Insert a key:
			cache.put("abcdefg", "abc");
			
			// Check it is present in the cache:
			cache.inCache("abcdefg");
			
			// Clear the *persistent* storage and check it is cleared from the *cache*:
			cache.clearPersistentStorage();
			assertFalse(cache.inCache("abcdefg"));
		}
	}
}
