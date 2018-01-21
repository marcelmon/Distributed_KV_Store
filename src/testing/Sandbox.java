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
		try {
			new LogSetup("logs/server.log", Level.ALL);
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
		
		final ServerSocket serverSocket;
		int port = 10123;
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());
            
            Thread bg = new Thread() {
            	@Override
            	public void run() {
            		try {          
	            		logger.info("Waiting for incoming connection...");
	                    Socket client = serverSocket.accept();
	                    
	                    System.out.println(client.getInetAddress().getHostAddress());
	                    System.out.println(client.getPort());
	                    
	        			OutputStream output = client.getOutputStream();
	        			InputStream input = client.getInputStream();
	        			
	        			try {
	        				serverRx_msg = new TLVMessage(input);
	        			} catch (KVMessage.StreamTimeoutException e) {
	        				
	        			}
	        			
	        			output.write(serverRx_msg.getBytes());
//	        			output.write(new byte[] {0,0});
            		} catch (IOException exc) {
            			logger.error("IOException");
            		} finally {
            			logger.info("Closing server socket...");
            			try {
            				serverSocket.close();
            			} catch (IOException e) {
            				logger.error("Unable to close server");
            			}
            		}
            	}
            };
            bg.start();
            
            Socket client = new Socket("localhost", port);
            OutputStream output = client.getOutputStream();
            InputStream input = client.getInputStream();
            
            TLVMessage txMsg = new TLVMessage(StatusType.GET, "a", null);
            output.write(txMsg.getBytes());
            
            try {
            	clientRx_msg = new TLVMessage(input);
            } catch (KVMessage.StreamTimeoutException e) {
            	
            }
			
			client.close();		
			
			assertTrue(serverRx_msg.equals(clientRx_msg));
            
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
        }
	}
	
	@Test
	public void test2() {
		try {
			new LogSetup("logs/server.log", Level.ALL);
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
		
		final ServerSocket serverSocket;
		int port = 10124;
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());
            
            Thread bg = new Thread() {
            	@Override
            	public void run() {
            		try {          
	            		logger.info("Waiting for incoming connection...");
	                    Socket client = serverSocket.accept();
	                    
	                    System.out.println(client.getInetAddress().getHostAddress());
	                    System.out.println(client.getPort());
	                    
	        			OutputStream output = client.getOutputStream();
	        			InputStream input = client.getInputStream();
	        			
	        			int len = input.available();
	        			byte[] buffer = new byte[len];
	        			input.read(buffer, 0, len);
	        			for (int i = 0; i < buffer.length; i++) {            	
	        				serverRx.add(buffer[i]);
	                    }
            		} catch (IOException exc) {
            			logger.error("IOException");
            		} finally {
            			logger.info("Closing server socket...");
            			try {
            				serverSocket.close();
            			} catch (IOException e) {
            				logger.error("Unable to close server");
            			}
            		}
            	}
            };
            bg.start();
            
            Socket client = new Socket("localhost", port);
            OutputStream output = client.getOutputStream();
            InputStream input = client.getInputStream();
            
            String msg = "ab";
            output.write(msg.getBytes());
            
            int len = input.available();
			byte[] buffer = new byte[len];
			input.read(buffer, 0, len);
			for (int i = 0; i < buffer.length; i++) {            	
				clientRx.add(buffer[i]);
            }
			
			client.close();		
			
			assertTrue(serverRx.equals(clientRx));
            
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
        }
	}
}
