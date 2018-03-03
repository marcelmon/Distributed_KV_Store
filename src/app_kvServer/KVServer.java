package app_kvServer;

import java.net.BindException;
import java.util.Map.Entry;
import java.io.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.comms.*;
import common.messages.*;
import common.messages.Message.StatusType;

public class KVServer implements IKVServer, ICommListener {
	protected static Logger logger = Logger.getRootLogger();
	protected final int desired_port;
	protected final CacheStrategy cacheStrategy;
	protected CommMod server;
	protected boolean running;
	protected ICache cache;
	protected final String name;
	protected final String zkHostname;
	protected final int zkPort;
	
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
        // TODO use name, zkHostname, zkPort
		this.name = name;
		this.desired_port = port;
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
		
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
		case "NONE":
			cache = new NoCache();
			cacheStrategy = CacheStrategy.NONE;
			break;
		default:
			System.out.println("Cache not recognized. Using dev mem-only cache!");
			cache = new MemOnlyCache(cacheSize);
			cacheStrategy = CacheStrategy.None;
			break;
		}
		cache = new ICacheDecorator(cache);
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
			if(args.length == 6){
				try {
					String name = args[0];
					int port = Integer.parseInt(args[1]);
					String zkHostname = args[2];
					int zkPort = Integer.parseInt(args[3]);					
					int cacheSize = Integer.parseInt(args[4]);
					String strategy = args[5];
					new KVServer(name, port, zkHostname, zkPort, cacheSize, strategy).run();
				} catch (Exception e) {
					logger.error("Error! " +
					e.getMessage());
				}
			}else {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <name> <port> <zkHostname> <zkPort> <cache size> <cache strategy(LRU,LFU,FIFO)>!");
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument! Not a number!");
			System.out.println("Usage: Server <name> <port> <zkHostname> <zkPort> <cache size> <cache strategy(LRU,LFU,FIFO)>!");
			System.exit(1);
		}
	}

	

	@Override
	public synchronized void OnKVMsgRcd(KVMessage msg, OutputStream client) {
		switch (msg.getStatus()) {
		case GET:
			try {
				try {
					String value = cache.get(msg.getKey());
//					System.out.println("Serving up key: " + msg.getKey());
					KVMessage resp = new KVMessage(StatusType.GET_SUCCESS, msg.getKey(), value);
					server.SendMessage(resp, client);
				} catch (ICache.KeyDoesntExistException e) {
//					System.out.println("Key doesn't exist: " + msg.getKey());
					KVMessage resp = new KVMessage(StatusType.GET_ERROR, msg.getKey(), null);
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

			// testWriteLock will check if there is a write lock and increment pendingPuts if no writeLock held
			if(!testWriteLock()){
				try {
					// reply that there is a write lock
					KVMessage resp = new KVMessage(StatusType.SERVER_WRITE_LOCK, msg.getKey(), null);
					server.SendMessage(resp, client);
				} catch (KVMessage.FormatException e) {
					System.out.println("Serious exception");
				} catch (Exception e) {
					//TODO log - this is serious
					System.out.println("Serious exception");
				}
				break;
			}

//			System.out.println("PUT");
			if (msg.getValue().isEmpty()) {     // deletion
				try {
					try {
						cache.delete(msg.getKey());
						decrementPendingPuts();
						KVMessage resp = new KVMessage(StatusType.DELETE_SUCCESS, msg.getKey(), null);
						server.SendMessage(resp, client);
					} catch (ICache.KeyDoesntExistException e) {
						decrementPendingPuts();
						KVMessage resp = new KVMessage(StatusType.DELETE_ERROR, msg.getKey(), null);
						server.SendMessage(resp, client);
					}
				} catch (KVMessage.FormatException e) {
					decrementPendingPuts();
					//TODO log - this is unexpected!
					System.out.println("Format exception");
				} catch (Exception e) {
					decrementPendingPuts();
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
					decrementPendingPuts();
					KVMessage resp = null;
					if (!failure) {
						if (insert) {
							resp = new KVMessage(StatusType.PUT_SUCCESS, msg.getKey(), null);
						} else {
							resp = new KVMessage(StatusType.PUT_UPDATE, msg.getKey(), null);
						}
					} else {
						resp = new KVMessage(StatusType.PUT_ERROR, msg.getKey(), null);
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
			throw new RuntimeException("Invalid request!"); // fatal because programmatic error
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


	protected boolean writeLock;
	protected int pendingPuts;

	/*
		Used by calls to this class in OnKVMsgRcd.
		See if there is a write lock currently and if not then increment pendingPuts and return true
		Return false otherwise
	*/
	public synchronized boolean testWriteLock(){
		if(!writeLock){
			pendingPuts++;
			return true;
		}
		return false;
	}

	// can be used by external service to check if the lock is currently active
	public boolean isWriteLocked(){
		return writeLock;
	}

	private Object pendingPutsLock = new Object();

	// used only for unit testing to manually set pending ++
	public void incrementPendingPuts(){
		synchronized(pendingPutsLock){
			pendingPuts++;
		}
	}

	public void decrementPendingPuts(){
		// the lockWrite() will set pending puts back to 0 if there's a timeout
		// check this didn't happen in the case a thread takes too long
		// need to have better error/failure handling here
		synchronized(pendingPutsLock){
			if(pendingPuts > 0){ 
				pendingPuts--;
			}
		}	
	}


	// currently only used for unit testing, returns integer value
	public int getPendingPuts(){
		return pendingPuts;
	}

	

	public synchronized void getLockWrite(){
		writeLock = true;
	}

    @Override
    public void lockWrite() {
		getLockWrite();
        int totalTime = 0;
        int maxTime = 10000; // millis, after this timeout, assume some failure and set pendingPuts = 0

        long startTimeMillis = System.currentTimeMillis();
        long finalTimeMillis = startTimeMillis + (long) maxTime;
        // wait for any threads to finish or the timeout
        while(pendingPuts > 0 && System.currentTimeMillis() <= finalTimeMillis){
        	try{
        		Thread.sleep(10);
        	} catch (InterruptedException e){
        		System.out.println("Thread sleep Exception");
        	}
        }
        if(pendingPuts > 0){
        	// LOG THIS
        	pendingPuts = 0;
        }
        
        return;
    }

    @Override
    public synchronized void unlockWrite() {
		writeLock = false;
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
        // TODO
        return false;
    }

	@Override
	public void consistentHasherUpdated(IConsistentHasher hasher) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void OnTuplesReceived(Entry<?, ?>[] tuples) {
		// TODO Auto-generated method stub
	}

	@Override
	public void OnTuplesRequest(Byte[] lower, Byte[] upper, OutputStream client) {
		// TODO Auto-generated method stub
	}
}
