package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class RunSandbox {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new KVServer("localhost", 50000, "localhost", 2181, 10, "FIFO");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Sandbox suite");
		clientSuite.addTestSuite(Sandbox.class); 
		return clientSuite;
	}
	
}
