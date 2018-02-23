package testing;

import java.util.Iterator;
import java.util.Map;
import java.lang.reflect.Constructor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

import org.junit.*;
import junit.framework.TestCase;
import app_kvServer.*;
import common.messages.Message.StatusType;

import java.security.MessageDigest;

public class HashRangeIteratorTest extends TestCase {
	protected ICache[] caches;
	protected int desiredCapacity;
	
	@Override
	public void setUp() {
		desiredCapacity = 1;
		caches = new ICache[1];
		caches[0] = new FIFOCache(desiredCapacity);

		// caches[0] = new MemOnlyCache(desiredCapacity);

		// caches[1] = new LFUCache(desiredCapacity);
		// caches[1].clearPersistentStorage();

		// caches[2] = new LRUCache(desiredCapacity);
		// caches[2].clearPersistentStorage();

		// caches[3] = new FIFOCache(desiredCapacity);
		// caches[3].clearPersistentStorage();

		// caches[4] = new NoCache();
		// caches[4].clearPersistentStorage();

	}
	



	public static byte[] subtractOne(byte[] A) {
	    int i = A.length - 1;  // start at the last position

	    // Looping from right to left
    	while(i >= 0 && A[i] == 0 ){
    		A[i] = (byte) (255 & 0xFF);
    		i--;
    	}
    	if(i < 0){ // have looped around (all were 0 and will now be 255)
			return A;
    	}
    	A[i] -= 1;
    	return A;
	}


	public static byte[] addOne(byte[] A) {

	    int i = A.length - 1;  // start at the last position
	    // Looping from right to left
    	while( i >= 0 && (A[i] & 0xFF) == 255 ){
    		if((A[i] & 0xFF) == 255){
    		}
    		A[i] = 0;
    		i--;
    	}
    	if(i < 0){ // have looped around (all were 255 and will now be all 0s)
			return A;
    	}
    	A[i] += 1;
    	return A;
	}


	public int firstBiggerThanSecond(byte[] first, byte[] second) {
        if(first.length > second.length) {
            return 1;
        }
        if(first.length < second.length) {
            return -1;
        }
        for(int index = 0; index < first.length; ++index ) {
            if( first[index] > second[index]) {
                return 1;
            }
            if( first[index] < second[index]) {
                return -1;
            }
        }
        return 0;
    }




    /*
		Will check that, even with only 1 cached value, specifying +/- 1 of the hash value will always work.
		Perform test for single value.
    */
	@Test
	public void testGetSingleHashedValue() throws Exception {
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};
		

		for (int i = 0; i < caches.length; ++i) {
			
			ArrayList<byte[]> allHashedKeys = new ArrayList<byte[]>();
			caches[i].clearPersistentStorage();
			
	        
			for (int j = 0; j < keys.length; ++j) {

				MessageDigest md = MessageDigest.getInstance("MD5");
		        byte[] keyHash = md.digest(keys[j].getBytes());

		        caches[i].put(keys[j], values[j]);
		        allHashedKeys.add(keyHash);
		    }

	        for (int k = 0; k < 3; ++k) {
				byte[] targetHash = allHashedKeys.get(k);
				byte[] targetHashPlusOne = addOne(targetHash.clone());
				byte[] targetHashMinusOne = subtractOne(targetHash.clone());

				ArrayList<Map.Entry<String, String>> returnKeys = new ArrayList<Map.Entry<String, String>>();

				Iterator<Map.Entry<String, String>> hashRangeIterator = caches[i].getHashRangeIterator(targetHashMinusOne, targetHashPlusOne);


				while(hashRangeIterator.hasNext() == true){
					returnKeys.add(hashRangeIterator.next());
				}

				assertTrue(returnKeys.size() == 1);
				SimpleEntry<String, String> retEntry = (SimpleEntry<String, String>) returnKeys.get(0);

				assertTrue(retEntry.getKey().equals(keys[k]));
				assertTrue(retEntry.getValue().equals(values[k]));
			}
		}


			


			


	}
	
}
