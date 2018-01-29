package common.comms;

import java.io.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import common.messages.KVMessage;
import common.messages.TLVMessage;
import logger.LogSetup;

public class CommMod implements ICommMod {
	protected String tx_ip;
	protected int tx_port;
	protected int rx_port;
	protected ICommListener listener;
	protected ServerSocket serverSocket;
	protected Thread serverThread;
	protected Socket clientSocket;
	
	protected class ClientThread extends Thread {
    	protected Socket client;
    	
    	public ClientThread(Socket client) {
    		super();
    		this.client = client;
    	}
    	
    	@Override
    	public void run() {
    		try {
        		InputStream input = client.getInputStream();
    			BufferedInputStream bufInput = new BufferedInputStream(input);
    			
    			while (!interrupted() && !client.isClosed()) {
    				if (input.available() > 0) {
    					try {
    						TLVMessage msg = new TLVMessage(bufInput);
    						if (listener != null) {
    							listener.OnMsgRcd(msg, client.getOutputStream());
    						} else {
    							System.out.println("Dropped a message.");
    						}
    					} catch (KVMessage.StreamTimeoutException e) {
    						// we don't care about timeouts here - just keep retrying forever
    					}
    				} 
    				try {
    					Thread.sleep(10);
    				} catch (InterruptedException e) {
    					// do nothing with it
    				}
    			}
    			
    			if (client.isClosed()) {
    				System.out.println("Client left.");
    			}
    		} catch (IOException exc) {
    			throw new RuntimeException("Server failure");
    		}
    	}
    };
	
	@Override
	public void StartServer(int port) throws Exception {
		// Create server:
		tx_port = port;
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setSoTimeout(50);
		} catch (IOException exc) {
			throw new Exception(exc.getMessage());
		}
		
		// Server listens in a new thread:
        serverThread = new Thread() {           	
        	@Override
        	public void run() {
        		try {    
        			while (!interrupted() && !serverSocket.isClosed()) {
//        				System.out.println("Server listening on port: " + tx_port);
        				try {
		                    Socket client = serverSocket.accept();
		                    new ClientThread(client).start();
        				} catch (SocketException e) {
        					if (serverSocket.isClosed()) {
        						// dont care - we're just closing
        					} else {
	        					e.printStackTrace();
	        					throw new RuntimeException("Unexpected error occurred");
        					}
        				} catch (SocketTimeoutException e) {
        					// do nothing
        				}
        			}    
//        			System.out.println("Server thread exiting");
        		} catch (IOException exc) {
        			exc.printStackTrace();
        			throw new RuntimeException("Server failure");
        		}
        	}
        };
        serverThread.start();
	}
	@Override
	public void StopServer() {
		if (serverThread != null) {
			serverThread.interrupt();
			try {
				serverSocket.close();
			} catch (IOException e) {
				System.out.println("Failed to close server socket!");
				e.printStackTrace();
			}
			try {
				serverThread.join(1000);
			} catch (InterruptedException e) {
				//do nothing
			}
		}
		
	}
	@Override
	public void SetListener(ICommListener listener) {
		this.listener = listener;		
	}
	@Override
	public void Connect(String ip, int port) throws UnknownHostException, Exception {
		try {
			this.tx_ip = ip;
			this.tx_port = port;
			clientSocket = new Socket(ip, port);
		} catch (UnknownHostException e) {
			throw e;
		} catch (IOException e) {
			throw new Exception(e.getMessage());
		}
	}
	
	@Override
	public void Disconnect() {
		if (clientSocket != null) {
			try {
				if (!clientSocket.isClosed()) {
					clientSocket.close();
				}
			} catch (IOException e) {
				System.out.println("Failed to close client socket.");
				//TODO log as a warning
			}
		}
		
	}
	
	@Override
	public void SendMessage(KVMessage msg, OutputStream client) throws Exception {
		client.write(msg.getBytes());
	}
	
	@Override
	public KVMessage SendMessage(KVMessage msg) throws KVMessage.StreamTimeoutException, Exception {
		if (clientSocket == null) {
			throw new Exception("Not yet connceted to server");
		}
		OutputStream output = clientSocket.getOutputStream();
        output.write(msg.getBytes());
        BufferedInputStream input = new BufferedInputStream(clientSocket.getInputStream());
        KVMessage response = new TLVMessage(input);
        return response;
	}
	
	

}
