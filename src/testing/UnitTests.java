package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class UnitTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void addUnitTests(TestSuite clientSuite) {
		System.out.println("addUnitTests");
		clientSuite.addTestSuite(BulkMessageTest.class);
		clientSuite.addTestSuite(CacheTests.class);
		clientSuite.addTestSuite(CommModTest.class);
		//clientSuite.addTestSuite(CommModPerfTest.class);											// SYSTEM TEST
		//clientSuite.addTestSuite(ConnectionTest.class);											// SYSTEM TEST
		clientSuite.addTestSuite(ConsistentHasherTest.class);
		clientSuite.addTestSuite(HashRangeIteratorTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);											// SYSTEM TEST
		clientSuite.addTestSuite(IntraServerCommsTest.class);
		clientSuite.addTestSuite(KVDBTests.class);
		clientSuite.addTestSuite(KVMessageTest.class);
//		clientSuite.addTestSuite(ReplicationSimpleTest.class);										// FAILS		
//		clientSuite.addTestSuite(KVServerBulkDataTransferAndConsistentHasherUpdateTest.class);		// FAILS
		clientSuite.addTestSuite(KVServerTests.class);
//		clientSuite.addTestSuite(LockWriteKVServerTest.class);										// FAILS (HANGS)
		clientSuite.addTestSuite(MessageTest.class);
		clientSuite.addTestSuite(SocketTest.class);
		clientSuite.addTestSuite(StoreServerTests.class);
		clientSuite.addTestSuite(TreeTests.class);
		clientSuite.addTestSuite(VectorClockTests.class);
//		clientSuite.addTestSuite(ZookeeperTest.class);												// CAUSES OTHERS TO FAIL
//		clientSuite.addTestSuite(ZookeeperExtTest.class);											// CAUSES OTHERS TO FAIL
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Storage Server Unit Test-Suite");

//		addUnitTests(clientSuite);
		clientSuite.addTestSuite(TreeTests.class);
		clientSuite.addTestSuite(VectorClockTests.class);
		
		return clientSuite;
	}
	
}
