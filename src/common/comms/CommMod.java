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
import java.util.Map;
import java.util.Map.Entry;

import javax.crypto.Mac;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import common.messages.BulkPackageMessage;
import common.messages.BulkRequestMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.Message.StatusType;
import common.messages.KVMessage;
import logger.LogSetup;

public class CommMod implements ICommMod {
	protected String client_ip;
	protected int server_port;
	protected int client_port;
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
    					bufInput.mark(1); // we'll only need to read a single byte
    					try {
    						// Read the tag without disturbing
    						int tag = bufInput.read();
    						bufInput.reset();    						
    						
    						Message msg = Message.getInstance(StatusType.values()[tag]);
    						msg.fromInputStream(bufInput);
    						if (listener != null) {

    							// TODO differentiate between Message types here
    							if (msg instanceof KVMessage)
    								listener.OnKVMsgRcd((KVMessage)msg, client.getOutputStream());
    							else if (msg instanceof BulkPackageMessage)
    								listener.OnTuplesReceived(((BulkPackageMessage) msg).getTuples());

    								// the output stream is to send back that the data is finished loading
    								// listener.OnTuplesReceived(((BulkPackageMessage) msg).getTuples(), client.getOutputStream());
    							else if (msg instanceof BulkRequestMessage) {
    								System.out.println("receiving request...");
    								BulkRequestMessage brm = (BulkRequestMessage) msg;
    								// client.getOutputStream().write(StatusType.BULK_PACKAGE.ordinal()); //DEBUG
    								listener.OnTuplesRequest(brm.getLower(), brm.getUpper(), client.getOutputStream());
    							} else {
    								throw new RuntimeException("Unknown child of Message detected");
    							}
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
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setSoTimeout(50);
			server_port = serverSocket.getLocalPort();
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
			this.client_ip = ip;
			this.client_port = port;
			System.out.println("***Connecting to " + ip + ":" + port);
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
	public boolean isConnected() {
		if (clientSocket != null) {
			return clientSocket.isConnected();
		}
		return false;
	}
	
	@Override
	public void SendMessage(KVMessage msg, OutputStream client) throws Exception {
		client.write(msg.getBytes());
		client.flush();
	}
	
	@Override
	public int GetPort() {
		return server_port;
	}
	
	@Override
	public KVMessage SendMessage(KVMessage msg) throws KVMessage.StreamTimeoutException, Exception {
		if (clientSocket == null) {
			throw new Exception("Not yet connected to server");
		}
		OutputStream output = clientSocket.getOutputStream();
        output.write(msg.getBytes());
        BufferedInputStream input = new BufferedInputStream(clientSocket.getInputStream());
        KVMessage response = new KVMessage(input);
        return response;
	}
	
	@Override
	public void SendTuples(Map.Entry<?, ?>[] tuples) throws Exception {
		if (clientSocket == null) {
			throw new Exception("Not yet connected to server");
		}
		if (tuples.length == 0) {
			throw new Exception("Empty set of tuples");
		}
		if (!(tuples[0].getKey() instanceof String)) {
			throw new Exception("Provided tuples don't have String keys!");
		}
		if (!(tuples[0].getValue() instanceof String)) {
			throw new Exception("Provided tuples don't have String keys!");
		}
			
		OutputStream output = clientSocket.getOutputStream();
		BulkPackageMessage msg = new BulkPackageMessage(tuples);

		output.write(msg.getBytes());
		// no response
	}
	
	@Override
	public Map.Entry<?, ?>[] GetTuples(Byte[] lower, Byte[] upper) throws Exception {
		if (clientSocket == null) {
			throw new Exception("Not yet connected to server");
		}
		OutputStream output = clientSocket.getOutputStream();
		BulkRequestMessage msg = new BulkRequestMessage(lower, upper);
		System.out.println("writing...");
		output.write(msg.getBytes());
		BufferedInputStream input = new BufferedInputStream(clientSocket.getInputStream());
		System.out.println("Requester waiting...");
		BulkPackageMessage response = new BulkPackageMessage(input);
        return response.getTuples();
	}
	@Override
	public void SendTuples(Map.Entry<?, ?>[] tuples, OutputStream client) throws Exception {
		// TODO Auto-generated method stub
	}


}
