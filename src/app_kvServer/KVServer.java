package app_kvServer;

import java.net.BindException;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.comms.*;
import common.messages.KVMessage;

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
		// TODO Auto-generated method stub
		this.port = port;
		if(strategy == "LRU") this.cacheStrategy = CacheStrategy.LRU;
		else if(strategy == "LFU") this.cacheStrategy = CacheStrategy.LFU;
		else if(strategy == "FIFO") this.cacheStrategy = CacheStrategy.FIFO;
		else this.cacheStrategy = CacheStrategy.None;
		cache = new MemOnlyCache(cacheSize);
		//TODO have a switch statement on "strategy"; throw an exception if not implemented
		//TODO in the switch, instantiate the cache and set the cachestrategy field
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
	public synchronized void OnMsgRcd(KVMessage msg) {
		// TODO The server has received a request from the client - do something with it
		switch (msg.getStatus()) {
		case GET:
			//TODO implement
			try {
				getKV(msg.getKey());
			} catch (Exception e) {
				logger.error("Error! " + "Key does not exist.");
			}
			break;
		case PUT:
			//TODO implement
			try {
				putKV(msg.getKey(), msg.getValue());
			} catch (Exception e) {
				e.getMessage();
			}
			break;
		default:
			//TODO log error
			// This is either an invalid status, or a SUCCESS/FAIL (which a client shouldn't be sending us)
			logger.error("Error! " + "Invalid status");
			break;
		}
	}

}
