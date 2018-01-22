package testing;

import org.junit.Test;
import common.messages.*;
import common.messages.KVMessage.*;
import junit.framework.TestCase;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.lang.Thread;

public class SocketTest extends TestCase {
	protected ArrayList<Byte> serverRx = new ArrayList<>();
	protected ArrayList<Byte> clientRx = new ArrayList<>();
	protected TLVMessage serverRx_msg;
	protected TLVMessage clientRx_msg;
	
	@Test
	public void testSocketEcho() {
    	try {
    		// Set up server:
    		int port = 10123;
            final ServerSocket serverSocket = new ServerSocket(port);
            
            // Server listens in a new thread:
            Thread bg = new Thread() {           	
            	@Override
            	public void run() {
            		try {          
	                    Socket client = serverSocket.accept();
	                    
	        			OutputStream output = client.getOutputStream();
	        			InputStream input = client.getInputStream();
	        			
	        			while (!interrupted()) {
	        				while (input.available() > 0) {
	        					output.write(input.read());
	        				} 
	        				try {
	        					Thread.sleep(10);
	        				} catch (InterruptedException e) {
	        					// do nothing with it
	        				}
	        			}
            		} catch (IOException exc) {
            			fail("IOException");
            		} finally {
            			try {
            				serverSocket.close();
            			} catch (IOException e) {
            				fail("Unable to close server");
            			}
            		}
            	}
            };
            bg.start();
            
            // Create the client connection in the main thread:
            Socket client = new Socket("localhost", port);
            OutputStream output = client.getOutputStream();
            InputStream input = client.getInputStream();
            
            // Transmit a message
            byte[] msg = {0, 1, 2, 3};
            output.write(msg);
            
            // Give the server some time to process:
            try {
            	Thread.sleep(50);
            } catch (InterruptedException e) {
            	fail("Thread.sleep() interrupted early"); 
            }
            
            // Signal to the server that the client is done:
            bg.interrupt();
            
            // Receive the response:
            assertTrue(input.available() == 4);
            for (int i = 0; i < 4; i++) {
            	assertTrue(input.read() == msg[i]);
            }
            assertTrue(input.available() == 0);
			
			client.close();		        
        } catch (IOException e) {
        	fail("Cannot open server socket");
        }
	}
	
	@Test
	public void testSocketEchoTLVMessage() {
    	try {
    		// Set up server:
    		int port = 10124;
            final ServerSocket serverSocket = new ServerSocket(port);
            
            // Server listens in a new thread:
            Thread bg = new Thread() {           	
            	@Override
            	public void run() {
            		try {          
	                    Socket client = serverSocket.accept();
	                    
	        			OutputStream output = client.getOutputStream();
	        			InputStream input = client.getInputStream();
	        			
	        			while (!interrupted()) {
	        				while (input.available() > 0) {
	        					output.write(input.read());
	        				} 
	        				try {
	        					Thread.sleep(10);
	        				} catch (InterruptedException e) {
	        					// do nothing with it
	        				}
	        			}
            		} catch (IOException exc) {
            			fail("IOException");
            		} finally {
            			try {
            				serverSocket.close();
            			} catch (IOException e) {
            				fail("Unable to close server");
            			}
            		}
            	}
            };
            bg.start();
            
            // Create the client connection in the main thread:
            Socket client = new Socket("localhost", port);
            OutputStream output = client.getOutputStream();
            InputStream input = client.getInputStream();
            
            // Transmit a message
            TLVMessage txMsg = new TLVMessage(StatusType.PUT, "abc", "def");
            output.write(txMsg.getBytes());
            
            // Give the server some time to process:
            try {
            	Thread.sleep(50);
            } catch (InterruptedException e) {
            	fail("Thread.sleep() interrupted early"); 
            }
            
            // Signal to the server that the client is done:
            bg.interrupt();
            
            // Receive the response:
            try {
	            TLVMessage rxMsg = new TLVMessage(new BufferedInputStream(input));
	            assertTrue(rxMsg.equals(txMsg));
	            assertTrue(input.available() == 0);
            } catch (KVMessage.StreamTimeoutException e) {
            	fail("stream timeout");
            }
			
			client.close();		        
        } catch (IOException e) {
        	fail("Cannot open server socket");
        }
	}
}
