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
		//clientSuite.addTestSuite(CommModPerfTest.class);
		//clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(ConsistentHasherTest.class);
		clientSuite.addTestSuite(HashRangeIteratorTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(IntraServerCommsTest.class);
		clientSuite.addTestSuite(KVDBTests.class);
		clientSuite.addTestSuite(KVMessageTest.class);
		clientSuite.addTestSuite(KVServerBulkDataTransferAndConsistentHasherUpdateTest.class);
		clientSuite.addTestSuite(LockWriteKVServerTest.class);
		clientSuite.addTestSuite(MessageTest.class);
		clientSuite.addTestSuite(SocketTest.class);
		clientSuite.addTestSuite(StoreServerTests.class);		
		clientSuite.addTestSuite(ZookeeperTest.class);
		//clientSuite.addTestSuite(ZookeeperExtTest.class);
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Storage Server Unit Test-Suite");

		addUnitTests(clientSuite);
		
		return clientSuite;
	}
	
}
