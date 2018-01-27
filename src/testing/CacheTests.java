package testing;

import java.util.Iterator;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;

public class CacheTests extends TestCase {
	protected ICache[] caches;
	protected int desiredCapacity;
	
	@Override
	public void setUp() {
		desiredCapacity = 10;
		caches = new ICache[1];
		caches[0] = new MemOnlyCache(desiredCapacity);
	}
	
	@Test
	public void testInsert() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testUpdate() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testDelete() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testInvalidGet() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testInvalidDelete() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testCapacity() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testClearCache() {
		try {
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
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testLoadCache() {
		// Contents to load:
		ArrayList<SimpleEntry<String, String>> list = new ArrayList<>();
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
}
