package testing;

import java.util.Arrays;

import org.junit.Test;
import common.messages.*;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import junit.runner.Version;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import java.lang.Thread;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Sandbox extends TestCase {
	protected static Logger logger = Logger.getRootLogger();
	protected ArrayList<Byte> serverRx = new ArrayList<>();
	protected ArrayList<Byte> clientRx = new ArrayList<>();
	protected TLVMessage serverRx_msg;
	protected TLVMessage clientRx_msg;
	
	@Test
	public void testMain() {
		
	}
}
