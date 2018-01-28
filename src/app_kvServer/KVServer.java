package app_kvServer;

import java.net.BindException;
import java.io.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.comms.*;
import common.messages.*;
import common.messages.KVMessage.StatusType;

public class KVServer implements IKVServer, ICommListener {
	protected static Logger logger = Logger.getRootLogger();
	protected final int port;
	protected final CacheStrategy cacheStrategy;
	protected CommMod server;
	protected boolean running;
	protected ICache cache;
	
	public void run() throws BindException, Exception {
		logger.info("Initialize server ...");
	   	try {
			server = new CommMod();
			server.SetListener(this);
			server.StartServer(port);
	        logger.info("Server listening on port: " 
	    	   		+ port);    
	    } catch (Exception e) {
	       	logger.error("Error! Cannot open server socket:");
	        if(e instanceof BindException){
	           	logger.error("Port " + port + " is already bound!");
	        }
	        throw e;
	    }
	}

	private boolean isRunning() {
		return running;
	}

	public void stopServer(){
        running = false;
        try {
			server.StopServer();
			server = null;
		} catch (Exception e) {
			logger.error("Error! " +
			"Unable to close socket on port: " + port, e);
		}
    }

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		
		switch (strategy) {
		case "LRU":
			throw new RuntimeException("LRU cache not implemented!");
		case "LFU":
			cache = new LFUCache(cacheSize);
			cacheStrategy = CacheStrategy.LFU;
			break;
		case "FIFO":
			throw new RuntimeException("FIFO cache not implemented!");
		default:
			System.out.println("Cache not recognized. Using dev mem-only cache!");
			cache = new MemOnlyCache(cacheSize);
			cacheStrategy = CacheStrategy.None;
			break;
		}
	}

	@Override
	public int getPort(){
		return port;
	}

	@Override
    public String getHostname(){
		//TODO what is the hostname supposed to be?
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return cacheStrategy;
	}

	@Override
    public int getCacheSize(){
		return cache.getCacheSize();
	}

	@Override
    public boolean inStorage(String key){
		return cache.inStorage(key);
	}

	@Override
    public boolean inCache(String key){
		return cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws ICache.KeyDoesntExistException {
		return cache.get(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		cache.put(key, value);
	}

	@Override
    public void clearCache() {
		cache.clearCache();
	}

	@Override
    public void clearStorage(){
		cache.clearPersistentStorage();
	}

	@Override
    public void kill(){
		//TODO verify functionality - this is supposed to kill the server without time to save.
		cache = null;
	}

	@Override
    public void close(){
		//TODO verify functionality - we want the cache to save to storage before killing it
		cache = null;
	}

	public static void main (String[] args) {
		try {
			new LogSetup("logs/server/server.log", Level.ALL);
			if(args.length == 3){
				try {
					int port = Integer.parseInt(args[0]);
					int cacheSize = Integer.parseInt(args[1]);
					String strategy = args[2];
					new KVServer(port, cacheSize, strategy).run();
				} catch (Exception e) {
					e.getMessage();
				}
			}else {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <cache strategy(None,LRU,LFU,FIFO)>!");
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}

	@Override
	public synchronized void OnMsgRcd(KVMessage msg, OutputStream client) {
		// TODO The server has received a request from the client - do something with it
		switch (msg.getStatus()) {
		case GET:
			try {
				try {
					String value = cache.get(msg.getKey());
					KVMessage resp = new TLVMessage(StatusType.GET_SUCCESS, msg.getKey(), value);
					server.SendMessage(resp, client);
				} catch (ICache.KeyDoesntExistException e) {
					System.out.println("Key doesn't exist");
					KVMessage resp = new TLVMessage(StatusType.GET_ERROR, msg.getKey(), "");
					server.SendMessage(resp, client);
				}
			} catch (KVMessage.FormatException e) {
				//TODO log - this is unexpected!
			} catch (Exception e) {
				//TODO log - this is serious
			}
			break;
		case PUT:
			try {
				boolean insert = cache.put(msg.getKey(), msg.getValue());
				KVMessage resp = null;
				if (insert) {
					resp = new TLVMessage(StatusType.PUT_SUCCESS, msg.getKey(), null);
				} else {
					resp = new TLVMessage(StatusType.PUT_UPDATE, msg.getKey(), null);
				}
				server.SendMessage(resp, client);
			} catch (KVMessage.FormatException e) {
				//TODO log - this is unexpected!
				System.out.println("Format exception");
			} catch (Exception e) {
				//TODO log - this is serious
				System.out.println("Serious exception");
			}
			break;
		default:
			//TODO log error
			// This is either an invalid status, or a SUCCESS/FAIL (which a client shouldn't be sending us)
			break;
		}
	}

}
