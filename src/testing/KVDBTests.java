package testing;

import java.util.Iterator;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;

import java.util.Random;


import java.io.*;

import java.util.Date;


import java.util.Arrays;

import java.util.Objects;

import java.lang.*;

public class KVDBTests extends TestCase {
	protected IKVDB[] kvdbs;
	protected String rootTestDataDirPath;
	protected String testDataDirPath;
	
	private static void recursiveFileDelete(String rootPath) throws IOException {

		File root = new File(rootPath);

 		if(root.exists()){
 			for (File childFile : root.listFiles()) {
	 
				if (childFile.isDirectory()) {
					recursiveFileDelete(childFile.getAbsolutePath());
				} else {
					if (!childFile.delete()) {
						throw new IOException();
					}
				}
			}
			if (!root.delete()) {
				throw new IOException();
			}
 		}
	}


	private static String createRandomTestDataDir(String rootTestDataDir) throws Exception {

		int startTimestamp = (int) (new Date().getTime()/1000);
		Random rand = new Random(); 
 		int randVal = rand.nextInt( (2147483647/2) - 1 ); 
		String dir_name_stamp = Integer.toString(startTimestamp) + "_" + Integer.toString(randVal);

		String testDataDirPath = rootTestDataDir + "/" + "test_data_" + dir_name_stamp;
		File testDataDir = new File(testDataDirPath);

		if (testDataDir.exists()) {
			throw new Exception("Test data dir " + testDataDirPath + " already exists.");
		}
		boolean result = false;
		try{
	        if(testDataDir.mkdir()){
	        	return testDataDirPath;
	        }
	    } 
	    catch(Exception e){
	        throw e;
	    }        
	    throw new Exception("Could not create test data dir " + testDataDirPath); 
	}

	@Override
	public void setUp() {

		// setup a root_test_data_dir @ "./root_test_data_dir_KVDB/" but make sure it is emptied direst
		// then create an internal directory with a unique timestamp
		String currentDirFile = System.getProperty("user.dir");

		this.rootTestDataDirPath = currentDirFile + "/root_test_data_dir_KVDB";

		try{
			recursiveFileDelete(this.rootTestDataDirPath);
		}
		catch(IOException e){
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}

		File rootTestDataDir = new File(this.rootTestDataDirPath);
		boolean result = false;
		try{
	        if(!rootTestDataDir.mkdir()){
	        	fail("Could not create root test data dir " + rootTestDataDirPath);
	        } else {
//	        	System.out.println("Created dir: " + rootTestDataDirPath);
	        }
	    } 
	    catch(Exception e){
	        fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
	    }        
		kvdbs = new IKVDB[1];
		try{
			kvdbs[0] = new FilePerKeyKVDB(this.rootTestDataDirPath);
		}
		catch(Exception e){
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}

	@Override
	public void tearDown(){
		try{
			recursiveFileDelete(this.rootTestDataDirPath);
//			System.out.println("Deleted dir: " + rootTestDataDirPath);
		}
		catch(IOException e){
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}

	
	@Test
	public void testInsert() {
		try {
			String[] keys = {"a", "b", "c"};
			String[] values = {"1", "2", "3"};
			assertTrue(keys.length == values.length);
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
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
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
				}
				
				// Update:
				assertTrue(kvdb.get("b").equals("2"));
				assertFalse(kvdb.put("b", "72")); // false for update
				assertTrue(kvdb.get("b").equals("72"));
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
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
				}
				
				// Delete:
				kvdb.delete("b");
				
				// Check not present:
				assertFalse(kvdb.inStorage("b"));
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
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
				}
				
				// Invalid get:
				boolean caught = false;
				try {
					kvdb.get("d");
				} catch (IKVDB.KeyDoesntExistException e) {
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
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
				}
				
				// Invalid delete:
				boolean caught = false;
				try {
					kvdb.delete("d");
				} catch (IKVDB.KeyDoesntExistException e) {
					caught = true;
				}
				assertTrue(caught);
			}
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	/*
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
	*/
	@Test
	public void testClearStorage() {
		try {
			String[] keys = {"a", "b", "c"};
			String[] values = {"1", "2", "3"};
			assertTrue(keys.length == values.length);
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
				}
				
				// Clear the storage:
				kvdb.clearStorage();
				
				// Check the keys are no longer present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));
				}
			}
		} catch (Exception e) {
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
	}
	
	@Test
	public void testIterator() {
		// Contents to load:
		try{
			ArrayList<SimpleEntry<String, String>> list = new ArrayList<>();
			String[] keys = {"a", "b", "c"};
			String[] values = {"1", "2", "3"};
			assertTrue(keys.length == values.length);
			for (int i = 0; i < keys.length; i++) {
				list.add(new SimpleEntry<String, String>(keys[i], values[i]));
			}
			
			for (IKVDB kvdb : kvdbs) {
				// Check not present:
				for (int i = 0; i < keys.length; i++) {
					assertFalse(kvdb.inStorage(keys[i]));	
				}
				
				// Insert
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.put(keys[i], values[i]));  // true for new
				}
				
				// Check present:
				for (int i = 0; i < keys.length; i++) {
					assertTrue(kvdb.inStorage(keys[i]));
					assertTrue(Objects.equals(kvdb.get(keys[i]), values[i]));
				}
				
				// Get iterator:
				Iterator<Map.Entry<String, String>> kvIterator = kvdb.iterator();
				
				int currentIndex = 0;
				// check that all key-values returned by the iterator are in the list
				while(kvIterator.hasNext()){
					Map.Entry<String, String> iteratedPair = kvIterator.next();

					// check that the key from iterator is in keys
					int originalIndex = Arrays.asList(keys).indexOf(iteratedPair.getKey());
					assertTrue(originalIndex >= 0);

					// check that the value for this key matches
					assertTrue(Objects.equals(iteratedPair.getValue(), values[originalIndex]));

					currentIndex++;	
				}

				// check that the iterator is as long as the keys
				assertTrue(currentIndex == keys.length);
			}
		}
		catch(Exception e){
			fail("Unexpected exception [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
		}
			
	}
	
	@Test
	public void testPersistence() {
		IKVDB db0 = new FilePerKeyKVDB(rootTestDataDirPath);
		assertFalse(db0.inStorage("a"));
		try {
			assertTrue(db0.put("a", "b")); // true => new
		} catch (Exception e) {
			fail(e.getMessage());
		}
		db0 = null;
		
		// New database object:
		IKVDB db1 = new FilePerKeyKVDB(rootTestDataDirPath);
		assertTrue(db1.inStorage("a"));
		try {
			assertTrue(db1.get("a").equals("b"));
		} catch (IKVDB.KeyDoesntExistException e) {
			fail("Key doesn't exist - highly unexpected.");
		}
	}
}
