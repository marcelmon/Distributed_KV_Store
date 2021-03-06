package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void addExtendedTests(TestSuite clientSuite) {
		System.out.println("addExtendedTests");
//		clientSuite.addTestSuite(BulkMessageTest.class);
//		clientSuite.addTestSuite(CacheTests.class);
//		clientSuite.addTestSuite(CommModTest.class);
		clientSuite.addTestSuite(CommModPerfTest.class);
		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(ConsistentHasherTest.class);
//		clientSuite.addTestSuite(HashRangeIteratorTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
//		clientSuite.addTestSuite(IntraServerCommsTest.class);
//		clientSuite.addTestSuite(KVDBTests.class);
//		clientSuite.addTestSuite(KVMessageTest.class);
//		clientSuite.addTestSuite(KVServerBulkDataTransferAndConsistentHasherUpdateTest.class);
//		clientSuite.addTestSuite(LockWriteKVServerTest.class);
//		clientSuite.addTestSuite(MessageTest.class);
//		clientSuite.addTestSuite(SocketTest.class);
//		clientSuite.addTestSuite(StoreServerTests.class);		
//		clientSuite.addTestSuite(ZookeeperTest.class);
		clientSuite.addTestSuite(ZookeeperExtTest.class);
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		
		UnitTests.addUnitTests(clientSuite);
		addExtendedTests(clientSuite);
		

		
		
		return clientSuite;
	}
	
}
