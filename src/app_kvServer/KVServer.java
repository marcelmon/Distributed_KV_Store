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
	protected final int desired_port;
	protected final CacheStrategy cacheStrategy;
	protected CommMod server;
	protected boolean running;
	protected ICache cache;
	
	public void run() throws BindException, Exception {
		logger.info("Initialize server ...");
	   	try {
			server = new CommMod();
			server.SetListener(this);
			server.StartServer(desired_port);
	    } catch (Exception e) {
	       	logger.error("Error! Cannot open server socket: " + desired_port);
	        if(e instanceof BindException){
	           	logger.error("Port " + desired_port + " is already bound!");
	        }
	        throw e;
	    }
	}

	private boolean isRunning() {
		return running;
	}

	/**
	 * Start KV Server at given port
         * @param name unique name of server
	 * @param port given port for storage server to operate
         * @param zkHostname hostname where zookeeper is running
         * @param zkPort port where zookeeper is running
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(String name, int port, String zkHostname, int zkPort, int cacheSize, String strategy) {
                //TODO use name, zkHostname, zkPort
		this.desired_port = port;
		
		switch (strategy) {
		case "LRU":
			cache = new LRUCache(cacheSize);
			cacheStrategy = CacheStrategy.LRU;
			break;
		case "LFU":
			cache = new LFUCache(cacheSize);
			cacheStrategy = CacheStrategy.LFU;
			break;
		case "FIFO":
			cache = new FIFOCache(cacheSize);
			cacheStrategy = CacheStrategy.FIFO;
			break;
		default:
			System.out.println("Cache not recognized. Using dev mem-only cache!");
			cache = new MemOnlyCache(cacheSize);
			cacheStrategy = CacheStrategy.None;
			break;
		}
	}

	@Override
	public int getPort(){
		return server.GetPort();
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
    public String getKV(String key) throws ICache.KeyDoesntExistException, ICache.StorageException, Exception {
		return cache.get(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
	        System.out.println("Server put:" + key + "," + value);
		cache.put(key, value);
	}

	@Override
    public void clearCache() {
		cache.clearCache();
	}

	@Override
    public void clearStorage(){
	    	System.out.println("CLEAR");
		cache.clearCache();
		cache.clearPersistentStorage();
	}

	@Override
    public void kill(){
		cache = null;
		server = null;
	}

	@Override
    public void close(){
		try {
			cache.writeThrough();
			server.StopServer();
			running = false;
		} catch (Exception e) {
			//TODO do something with this
			System.out.println("Failed to close cache cleanly");
			e.printStackTrace();
		}
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
					logger.error("Error! " +
					e.getMessage());
				}
			}else {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <cache strategy(LRU,LFU,FIFO)>!");
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
//					System.out.println("Serving up key: " + msg.getKey());
					KVMessage resp = new TLVMessage(StatusType.GET_SUCCESS, msg.getKey(), value);
					server.SendMessage(resp, client);
				} catch (ICache.KeyDoesntExistException e) {
//					System.out.println("Key doesn't exist: " + msg.getKey());
					KVMessage resp = new TLVMessage(StatusType.GET_ERROR, msg.getKey(), null);
					server.SendMessage(resp, client);
				}
			} catch (KVMessage.FormatException e) {
				e.printStackTrace();
				//TODO log - this is unexpected!
				logger.error("Error! " +
				e.getMessage());
			} catch (Exception e) {
				//TODO log - this is serious
				logger.error("Error! " +
				e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
			break;
		case PUT:
//			System.out.println("PUT");
			if (msg.getValue().isEmpty()) {     // deletion
				try {
					try {
						cache.delete(msg.getKey());
						KVMessage resp = new TLVMessage(StatusType.DELETE_SUCCESS, msg.getKey(), null);
						server.SendMessage(resp, client);
					} catch (ICache.KeyDoesntExistException e) {
						KVMessage resp = new TLVMessage(StatusType.DELETE_ERROR, msg.getKey(), null);
						server.SendMessage(resp, client);
					}
				} catch (KVMessage.FormatException e) {
					//TODO log - this is unexpected!
					System.out.println("Format exception");
				} catch (Exception e) {
					//TODO log - this is serious
					System.out.println("Serious exception");
				}
			} else {							//insert/update
				try {
					boolean insert = false;
					boolean failure = false;
					try { 
    					    insert = cache.put(msg.getKey(), msg.getValue());
					} catch (Exception e) {
						failure = true;
					}
					KVMessage resp = null;
					if (!failure) {
					if (insert) {
						resp = new TLVMessage(StatusType.PUT_SUCCESS, msg.getKey(), null);
					} else {
						resp = new TLVMessage(StatusType.PUT_UPDATE, msg.getKey(), null);
					}
					} else {
						resp = new TLVMessage(StatusType.PUT_ERROR, msg.getKey(), null);
					}
					server.SendMessage(resp, client);
				} catch (KVMessage.FormatException e) {
					//TODO log - this is unexpected!
					System.out.println("Format exception");
				} catch (Exception e) {
					//TODO log - this is serious
					e.printStackTrace();
					System.out.println("Serious exception");
				}
			}
			break;
		default:
			//TODO log error
			System.out.println("Invalid request");
			// This is either an invalid status, or a SUCCESS/FAIL (which a client shouldn't be sending us)
			logger.error("Error! " + "Invalid status");
			break;
		}
	}

	@Override
	public void start() {
		// TODO
	}

        @Override
        public void stop() {
            // TODO
	}

        @Override
        public void lockWrite() {
            // TODO
        }

        @Override
        public void unlockWrite() {
            // TODO
        }

        @Override
        public boolean moveData(String[] hashRange, String targetName) throws Exception {
            // TODO
            return false;
        }

}
