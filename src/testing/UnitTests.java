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
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Storage Server Unit Test-Suite");

		clientSuite.addTestSuite(TLVMessageTest.class);
		clientSuite.addTestSuite(SocketTest.class);
		clientSuite.addTestSuite(CommModTest.class);
		clientSuite.addTestSuite(KVDBTests.class);
		clientSuite.addTestSuite(CacheTests.class);
		clientSuite.addTestSuite(StoreServerTests.class);		
		clientSuite.addTestSuite(ZookeeperTest.class);
		clientSuite.addTestSuite(ConsistentHasherTest.class);
		
		return clientSuite;
	}
	
}
