package app_kvServer;

import java.net.BindException;
import java.util.Map.Entry;
import java.io.*;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.comms.*;
import common.messages.*;
import common.messages.Message.FormatException;
import common.messages.Message.StatusType;
import common.comms.IConsistentHasher.HashComparator;
import common.comms.IConsistentHasher.ServerRecord;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.security.*;




public class KVServer implements IKVServer, ICommListener {
	protected static Logger logger = Logger.getRootLogger();
	
	
	protected boolean isStarted;
	protected boolean pendingRunning;
	
	protected final int REPLICATION_FACTOR = 2; 


	protected final int desired_port;
	protected final String name;
	protected final CacheStrategy cacheStrategy;

	protected ICache cache;
	protected CommMod server;
	protected IIntraServerComms isc;
	protected IConsistentHasher hasher;

	protected String zkAddr;
	protected final String zkHostname;
	protected final int zkPort;
	
	public LockWrite lockWrite;

	protected ConsistentHasher.ServerRecord oldBelow;

	public IConsistentHasher getConsistentHasher(){
		return hasher;
	}

	public IIntraServerComms getIntraServerComms(){
		return isc;
	}

	public CommMod getCommMod(){
		return server;
	}


	public void run() throws BindException, Exception {
		logger.info("Initialize server ...");


		lockWrite = new LockWrite();

		zkAddr = zkHostname + ":" + Integer.toString(zkPort);


		isStarted = false;
		pendingRunning = false;

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
	    try{

			isc = new IntraServerComms(zkAddr, name, desired_port);

			// this will register this KVServer as a listener 
			// the consistentHasherUpdated will be called
			isc.register(this); // will ensure the consistent hasher updated is called


	    } catch (Exception e) {
	       	logger.error("Error! zookeeper probs");
	       
	        throw e;
	    }
	}

	public boolean isStarted() {
		return isStarted;
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

		isStarted = false;
        // TODO use name, zkHostname, zkPort
		this.name = name;
		this.desired_port = port;
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;

		// for non-mem-only use directory : name + ":" + port
		switch (strategy) {
		case "LRU":
			cache = new LRUCache(cacheSize, name + ":" + port);
			cacheStrategy = CacheStrategy.LRU;
			break;
		case "LFU":
			cache = new LFUCache(cacheSize, name + ":" + port);
			cacheStrategy = CacheStrategy.LFU;
			break;
		case "FIFO":
			cache = new FIFOCache(cacheSize, name + ":" + port);
			cacheStrategy = CacheStrategy.FIFO;
			break;
		case "NONE":
			cache = new NoCache(name + ":" + port);
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
		return name;
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
		} catch (Exception e) {
			//TODO do something with this
			System.out.println("Failed to close cache cleanly");
			e.printStackTrace();
		}
		cache = null;

		try{
			isc.close();
		} catch(Exception e){
			System.out.println("Failed to close isc cleanly");
			e.printStackTrace();
		}
		
		oldBelow = null;
		isStarted = false;
		isc = null;
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

		String logHeader = name+":"+desired_port +" OnKVMsgRcd () " + msg.getStatus();

		if(isStarted == false){
			try {
				// a stopped server will still have a consistent hash ring, send it back to the client
				KVMessage resp = new KVMessage(StatusType.SERVER_STOPPED, hasher.toString(), null);
				try{
					server.SendMessage(resp, client);
				} catch(Exception ee){
					logger.error("Error! " + ee.getMessage());
					throw new RuntimeException(ee.getMessage());
				}
			} catch (KVMessage.FormatException e){
				e.printStackTrace();
				logger.error("Error! " + e.getMessage());
			}
			return;
		}

		ConsistentHasher.ServerRecord targetServer = hasher.mapKey(msg.getKey());
		System.out.println("Target port: " + targetServer.port);
		if(msg.getStatus().equals(StatusType.GET) || msg.getStatus().equals(StatusType.PUT)){
			if(targetServer == null){
				throw new RuntimeException(logHeader + " - No servers registered.");
			}
			else if(!targetServer.hostname.equals(name) || targetServer.port != getPort()){
				try {
					KVMessage resp = new KVMessage(StatusType.SERVER_NOT_RESPONSIBLE, hasher.toString(), null);
					try{
						server.SendMessage(resp, client);
					} catch(Exception ee){
						logger.error("Error! " + ee.getMessage());
						throw new RuntimeException(ee.getMessage());
					}
				} catch (KVMessage.FormatException e){
					e.printStackTrace();
					logger.error("Error! " + e.getMessage());
				}
				return;
			}
		} 

		switch (msg.getStatus()) {
			case TRANSFER_COMPLETE:
				OnTransferCompleteReceived(msg, client);
				break;
			case TRANSFER_COMPLETE_ACK:
				OnTransferCompleteACKReceived(msg, client);
				break;
			case TRANSFER_COMPLETE_NACK:
				OnTransferCompleteNACKReceived(msg, client);
				break;
			case GET:
				try {
					try {
						String value = cache.get(msg.getKey());
						KVMessage resp = new KVMessage(StatusType.GET_SUCCESS, msg.getKey(), value);
						server.SendMessage(resp, client);
					} catch (ICache.KeyDoesntExistException e) {
						KVMessage resp = new KVMessage(StatusType.GET_ERROR, msg.getKey(), null);
						server.SendMessage(resp, client);
					}
				} catch (KVMessage.FormatException e) {
					e.printStackTrace();
					//TODO log - this is unexpected!
					logger.error("Error! " + e.getMessage());
				} catch (Exception e) {
					//TODO log - this is serious
					logger.error("Error! " + e.getMessage());
					throw new RuntimeException(e.getMessage());
				}
				break;
			case PUT:
			case FORCE_PUT:
				KVMessage resp = null;
				boolean isLockWrite = lockWrite.testIncrementPuts();
				try{
					// testIncrementPuts will check if there is a write lock and increment pendingPuts if no writeLock held
					if(!isLockWrite){
						try {
							// reply that there is a write lock
							resp = new KVMessage(StatusType.SERVER_WRITE_LOCK, msg.getKey(), null);
						} catch (KVMessage.FormatException e) {
							System.out.println("Serious exception");
						}
					}
					else if (msg.getValue().isEmpty()) {     // deletion
						try {
							try {
								cache.delete(msg.getKey());
								resp = new KVMessage(StatusType.DELETE_SUCCESS, msg.getKey(), null);
								
							} catch (ICache.KeyDoesntExistException e) {
								resp = new KVMessage(StatusType.DELETE_ERROR, msg.getKey(), null);
							}
						} catch (KVMessage.FormatException e) {
							//TODO log - this is unexpected!
							System.out.println("Format exception");
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

							if (!failure) {
								if (insert) {
									resp = new KVMessage(StatusType.PUT_SUCCESS, msg.getKey(), null);
								} else {
									resp = new KVMessage(StatusType.PUT_UPDATE, msg.getKey(), null);
								}
							} else {
								resp = new KVMessage(StatusType.PUT_ERROR, msg.getKey(), null);
							}
						} catch (KVMessage.FormatException e) {
							//TODO log - this is unexpected!
							System.out.println("Format exception");
						}
					}

					if(resp != null){
						try{
							server.SendMessage(resp, client);
						} catch (Exception e) {
							lockWrite.decrementPendingPuts();
							//TODO log - this is serious
							System.out.println("Serious exception");
						}
						
					}
				} catch (Exception ee){

					System.out.println("Serious exception");
				}
					
				lockWrite.decrementPendingPuts();
				
				// Forward to replicated servers:
				// We don't want to forward a FORCE_PUT as this will create a cycle!
				if (msg.getStatus().equals(StatusType.PUT)) {
						List<ServerRecord> redundantDestinations = hasher.mapKeyRedundant(msg.getKey(), REPLICATION_FACTOR);
						try {
							KVMessage forcedMsg = new KVMessage(StatusType.FORCE_PUT, msg.getKey(), msg.getValue());
							CommMod forwarder = new CommMod();
							for (ServerRecord rec : redundantDestinations) {
								try {
									forwarder.Connect(rec.hostname, rec.port);
									KVMessage forcedMsgResp = forwarder.SendMessage(forcedMsg);
									if (forcedMsgResp.getStatus() != StatusType.PUT_SUCCESS &&
										forcedMsgResp.getStatus() != StatusType.PUT_UPDATE &&
										forcedMsgResp.getStatus() != StatusType.DELETE_SUCCESS) {
											// TODO error
											System.out.println("Error: FORCE_PUT response unexpected!");
									}
								} catch (Exception e) {
									System.out.println("Unexpected error while forwarding put: " + e.getMessage());
									//TODO error - non fatal globally but failed to forward
								}
							}
						} catch (FormatException e) {
							throw new RuntimeException(e.getMessage()); // fundamental flaw
						}
				}
			
					
				break;
			default:
				throw new RuntimeException("Invalid request!"); // fatal because programmatic error
		}
	}

	public boolean isAddedToHashRing = false;

	@Override
	public void start() throws Exception {
		String logHeader = name+":"+desired_port + " start()";
		System.out.println(logHeader + " - called");
		
		if(isStarted){
			throw new Exception("ERROR ALREADY STARTED!");
		}
		isStarted = true;

		//TODO test if this works
//		// request data from neighbour above (clockwise)
//		IConsistentHasher.ServerRecord me = new IConsistentHasher.ServerRecord(name, desired_port);
//
//		ServerRecord txServer = new ServerRecord(null);
//		List<Byte> clockwise = new ArrayList<Byte>();
//		List<Byte> counterclockwise = new ArrayList<Byte>();
//		hasher.preAddServer(me, txServer, clockwise, counterclockwise);
//
//		if(txServer.hostname != null){
//
//			// request some tuples
//			CommMod bulkClient = new CommMod();
//			bulkClient.Connect(txServer.hostname, txServer.port); 
//			Map.Entry<?, ?>[] receivedTuples = bulkClient.GetTuples(clockwise.toArray(new Byte[clockwise.size()]), counterclockwise.toArray(new Byte[counterclockwise.size()]));
//	
//			// put the tuples directly to the cache (can go directly to disk)
//			if(receivedTuples.length > 0){
//				for (Map.Entry<?, ?> tuple : receivedTuples) {
//					cache.put((String) tuple.getKey(), (String) tuple.getValue());
//					// cache.kvdb.put((String) tuple.getKey(), (String) tuple.getValue());
//				}
//			}
//		}

		isc.addServer();
	}

	@Override
	public void stop() {
    	isStarted = false;
    	try{
    		// Remove this server:
    		isc.removeServer();
    		
    		// Get destination server:
    		ServerRecord me = new ServerRecord(getHostname(), getPort());
    		ServerRecord rxServer = new ServerRecord(new Byte[0]);
    		hasher.preRemoveServer(me, rxServer);
    		
    		// Populate a list of the tuples this server was responsible for:
    		List<Entry<String, String>> tuples = cache.getTuples();
//	    		List<Entry<String, String>> tuples = cache.getTuples(me.hash, rxServer.hash);
    		
    		// Send tuples:
    		CommMod comm = new CommMod();
    		comm.Connect(rxServer.hostname, rxServer.port);
    		comm.SendTuples(tuples.toArray(new Entry<?, ?>[0]));
    	} catch (IIntraServerComms.NotYetRegisteredException e){
    		System.out.println("ERRORR WITH NotYetRegisteredException");
    	} catch (Exception e){
    		System.out.println("ERRORR WITH Exception");
    	}
	}





	public class LockWrite {

		protected boolean lockWrite;
		protected int pendingPuts;

		public int totalLockWrites;

		private Object pendingPutsLockObj;
		private Object lockWriteObj;


		public LockWrite(){
			totalLockWrites = 0;
			pendingPutsLockObj = new Object();
			lockWriteObj = new Object();
		}


		// can be used by external service to check if the lock is currently active
		public boolean isLockWrite(){
			return lockWrite;
		}

		

		public void incrementPendingPuts(){
			synchronized(pendingPutsLockObj){
				pendingPuts++;
			}
		}

		public int getTotalLockWrites(){
			return totalLockWrites;
		}

		// currently only used for unit testing, returns integer value
		public int getPendingPuts(){
			return pendingPuts;
		}

		public void decrementPendingPuts(){
			// the lockWrite() will set pending puts back to 0 if there's a timeout
			// check this didn't happen in the case a thread takes too long
			// need to have better error/failure handling here
			synchronized(pendingPutsLockObj){
				if(pendingPuts > 0){ 
					pendingPuts--;
				}
			}	
		}


		/*
			Used by calls to this class in OnKVMsgRcd.
			See if there is a write lock currently and if not then increment pendingPuts and return true
			Return false if there is locked
		*/
		public synchronized boolean testIncrementPuts(){
			synchronized(lockWriteObj){
				if(!lockWrite){
					incrementPendingPuts();
					return true;
				}
			}
			return false;
		}

		

	    public void unlockWrite() {
	    	synchronized(lockWriteObj){
	    		if(totalLockWrites <= 0){
	    			return;
	    		}
	    		totalLockWrites--;
		    	if(totalLockWrites <= 0){
		    		lockWrite = false;
		    		totalLockWrites = 0;
		    	}
	    	} 				
	    }

	    // if not writeLock return false, else try unlocking, if unlocked then return true, return false if there's still another lock
	    /*
			@return true  if was locked and now is unlocked
			@return false if was not locked or was locked but still is because still pending locks (totalLockWrites > 0)
	    */
	    public boolean testUnlockWrite() {
	    	synchronized(lockWriteObj){
	    		if(lockWrite == true){
	    			totalLockWrites--;
			    	if(totalLockWrites == 0){
			    		lockWrite = false;
			    		return true;
			    	}
	    		}
	    		return false;
	    	}
	    }

		
		// poll until pending puts is 0 or timeoutMillis is reached
		// return true if no timeout, false if timeout (but still set pendingPuts = 0 for now)
		public boolean waitForPendingPuts(int timeoutMillis){
			if(pendingPuts == 0){
				return true;
			}
			
	        long finalTimeMillis = System.currentTimeMillis() + timeoutMillis;
	        // wait for any threads to finish or the timeout
	        while(pendingPuts > 0) {

	         	if(System.currentTimeMillis() >= finalTimeMillis){
	         		System.out.println("TIMEOUT WAITING FOR LOCK!");
	         		pendingPuts = 0;
	         		return false; // should maybe unlock and THROW EXCEPTION instead
	         	}
	        	try{
	        		Thread.sleep(10);
	        	} catch (InterruptedException e){
	        		System.out.println("Thread sleep Exception");
	        	}
	        }  
	        return true;
		}


	    // checks if already locked, if not then locks and returns true, return false if already locked
		public boolean testLockWrite(){

			synchronized(lockWriteObj){
				if(lockWrite == true){
					return false; // already locked
				}
				lockWrite = true;
				totalLockWrites++;
			}
	        waitForPendingPuts(10000); 
	        return true;
		}
		

		// will writeLock = true and wait until all puts complete or timeout
	    public void lockWrite() {
			
			synchronized(lockWriteObj){
				// if(writeLock == true){
				// 	// already a write lock
				// }
				lockWrite = true;
				totalLockWrites++;
			}

			waitForPendingPuts(10000);
	    }
	}




	@Override
    public void unlockWrite() {
		lockWrite.unlockWrite();
    }


    @Override
    public void lockWrite() {
    	lockWrite.lockWrite();
    }

    

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {


    	
    	// convert Byte[] to byte[] for ics2.call
		byte[] byteMinHash = Base64.getDecoder().decode(hashRange[0]);

		byte[] byteMaxHash = Base64.getDecoder().decode(hashRange[1]);

		String logHeader = name+":"+desired_port
			+" moveData (), target : " + targetName 
			+" min hash:" + Base64.getEncoder().encodeToString(byteMinHash)
			+" max hash:" + Base64.getEncoder().encodeToString(byteMaxHash);

		logger.debug( logHeader );
		System.out.println( logHeader );


    	int currentPacketSize = 0;
        int maxPacketSize = 100000;
        int index = 0;

        String[] targetHostPort = targetName.split(":");

    	if(true) {

    		Iterator<Map.Entry<String, String>> iter = null;
    		if(cache == null){
				throw new RuntimeException("Null cache \n\n\n\n\n" + logHeader);
			}
    		// try{
    			iter = cache.getHashRangeIterator(byteMinHash, byteMaxHash);
    // 		} catch(Exception e){

    // 			StringWriter sw = new StringWriter();
				// e.printStackTrace(new PrintWriter(sw));
				// String exceptionAsString = sw.toString();

				// System.out.println("HASH RANGE ITER PROBS exce stack " + exceptionAsString);
    // 		}

	        while(iter.hasNext()){

	        	ArrayList<Map.Entry<String, String>> tuples = new ArrayList<Map.Entry<String, String>>();

	        	while(iter.hasNext() && currentPacketSize <= maxPacketSize){

		        	Map.Entry<String, String> currentEntry = iter.next();
		        	tuples.add(currentEntry);
		        	// assumes that tuples fill exactly the right amount of space, we could have a value too large
		        	currentPacketSize += currentEntry.getKey().length() + currentEntry.getValue().length();
		        }

		        AbstractMap.SimpleEntry<?, ?>[] tupleArray = new AbstractMap.SimpleEntry<?,?>[tuples.size()];


		        for (int i = 0; i < tuples.size(); i++) {

		        	tupleArray[i] = new AbstractMap.SimpleEntry<String, String>(tuples.get(i).getKey(), tuples.get(i).getValue());

		        }
		        try{
		        	CommMod destServer = new CommMod();
					destServer.Connect(targetHostPort[0], Integer.parseInt(targetHostPort[1]));
		        	destServer.SendTuples(tupleArray);
		        } catch (Exception e){
		        	System.out.println("Move data kv server send tuples exception : " + e.getMessage());
		        	throw e;
		        }		        	
	        }
	        return true;
    	}

    	if(!lockWrite.testLockWrite()){

    	}

        Iterator<Map.Entry<String, String>> iter = cache.getHashRangeIterator(hashRange[0].getBytes(), hashRange[1].getBytes());


        while(iter.hasNext()){
        	ArrayList<Map.Entry<String, String>> tuples = new ArrayList<Map.Entry<String, String>>();

        	while(iter.hasNext() && currentPacketSize <= maxPacketSize){

	        	Map.Entry<String, String> currentEntry = iter.next();

	        	tuples.add(currentEntry);

	        	// assumes that tuples fill exactly the right amount of space
	        	currentPacketSize += currentEntry.getKey().length() + currentEntry.getValue().length();
	        	
	        }
	        try{
	        	CommMod destServer = new CommMod();
				destServer.Connect(targetHostPort[0], Integer.parseInt(targetHostPort[1]));
	        	destServer.SendTuples((Map.Entry<String, String>[]) tuples.toArray());
	        } catch (Exception e){
	        	System.out.println("Move data kv server send tuples exception : " + e.getMessage());
	        	throw e;
	        }
	        	
        }

        unlockWrite();
	     
        return false;
    }

    
    // TODO : LOCK METHOD WITH STATEMENT LOCK

    
    public synchronized boolean getNeighourData() throws Exception {
    	if(this.hasher == null){
    		return true;
    	}
    	if(hasher.getServerList().length == 0){
			return true;	
		}
    	// in startup - request data from neighbour 
		ArrayList<Byte> newRangeLower = new ArrayList<Byte>();
		ArrayList<Byte> newRangeUpper = new ArrayList<Byte>();
		ConsistentHasher.ServerRecord txServer = new ConsistentHasher.ServerRecord(null);

		ConsistentHasher.ServerRecord me = new ConsistentHasher.ServerRecord(name, desired_port);


		hasher.preAddServer(me, txServer, newRangeLower, newRangeUpper);

		if(txServer.hostname != null){

			// Make call to get data, use client functions instead of listening server
			CommMod bulkClient = new CommMod();
			bulkClient.Connect(txServer.hostname, txServer.port);

			// need to check here for some kind of error
			// if the other server was already writeLock() then we would have to wait
			Map.Entry<?, ?>[] receivedTuples = bulkClient.GetTuples(newRangeLower.toArray(new Byte[newRangeLower.size()]), newRangeUpper.toArray(new Byte[newRangeUpper.size()]));
	
			if(receivedTuples.length > 0){
				for (Map.Entry<?, ?> tuple : receivedTuples) {
					cache.put((String) tuple.getKey(), (String) tuple.getValue());
				}
			}

			// CONSIDER SENDING A LAST REPLY TO STATE COMPLETE AND WAIT FOR REPLY
			// can add some info if needed such as server name and hash range
			KVMessage transferComplete = new KVMessage(StatusType.TRANSFER_COMPLETE, "", null);
			KVMessage tcResp = bulkClient.SendMessage(transferComplete);

			if(tcResp.getStatus() == StatusType.TRANSFER_COMPLETE_ACK){
				// the server sending us the data did not time out while we were loading, okay to move on
				
				return true;
			}
			else if(tcResp.getStatus() == StatusType.TRANSFER_COMPLETE_NACK){
				// the sending server timed out and so will have already unlockWrite()
				// we can fail here or start over, failure is probs a better idea
				System.out.println("HERE WHY?");
			}
			else{
				// weird stuff going on
				System.out.println("HERE WHY 2?");
			}

			return false;
		}
		return true;
    }

    public synchronized boolean checkIfAdded(String queryHost, int queryPort, IConsistentHasher hasher){
    	ConsistentHasher.ServerRecord[] newList = hasher.getServerList();
		for(ConsistentHasher.ServerRecord newRec : newList){

			if(newRec.hostname.equals(queryHost) && newRec.port == queryPort){
				// was found
				return true;
			}
		}
		return false;
    }


    // if a server was added between us and the previous lowest then we can delete keys, also remove write lock
    public synchronized boolean checkNewServerHashRange(IConsistentHasher newHasher){
    	
    	String logHeader = name+":"+desired_port +" checkNewServerHashRange () ";

		System.out.println(logHeader + " - called");

		boolean above = false;
		IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();
		ConsistentHasher.ServerRecord me = new ConsistentHasher.ServerRecord(name, desired_port);
		ConsistentHasher.ServerRecord newBelow = ((ConsistentHasher) newHasher).FindServer(me, above);

		if(oldBelow == null){

			System.out.println(logHeader + " - is old below null? yes.");

			oldBelow = newBelow;
			return false;
		}
		


		boolean isBetween = false;
		if(comp.compare(oldBelow.hash, newBelow.hash) != 0){

			System.out.println(logHeader + " comp not 0");
			

			// greater than previous below and less than me
			if(comp.compare(newBelow.hash, oldBelow.hash) > 0){
				System.out.println(logHeader + " new greater old");
				if(comp.compare(newBelow.hash, me.hash) < 0){
					System.out.println(logHeader + " new greater old0 and less than me");
					isBetween = true;
				}
			}

			else if(comp.compare(oldBelow.hash, me.hash) > 0){ // passes 0
				System.out.println(logHeader + " old b greater me ");
				// less than me or greater than old
				if (comp.compare(newBelow.hash, me.hash) < 0 || comp.compare(newBelow.hash, oldBelow.hash) > 0) {
					System.out.println(logHeader + " old b greater me and or new blow less me or greater old");
					isBetween = true;
				}
			}

			oldBelow = newBelow;
		}

		System.out.println(logHeader + " - in between : " + isBetween);

		if(isBetween){

			

			byte[] oldBelowByte = new byte[oldBelow.hash.length];
			for (int i = 0; i < oldBelow.hash.length; i++) oldBelowByte[i] = oldBelow.hash[i];

			byte[] newBelowByte = new byte[newBelow.hash.length];
			for (int i = 0; i < newBelow.hash.length; i++) newBelowByte[i] = newBelow.hash[i];

			Iterator<Map.Entry<String, String>> iter = cache.getHashRangeIterator(oldBelowByte, newBelowByte);
			Map.Entry<String,String> item = null;
			while((item = iter.next()) != null){

				try{
					cache.delete(item.getKey());	
				} catch (ICache.KeyDoesntExistException e){
					System.out.println(logHeader + " key doesnt exist");
				} catch(Exception e){
					System.out.println(logHeader + " exceptiom " + e.getMessage());
				}

			}
		}

		return true;
    }



    public boolean testRingShrunk(
    	ConsistentHasher.ServerRecord me,
    	ConsistentHasher.ServerRecord oldLowerHashBound,
    	ConsistentHasher.ServerRecord newLowerHashBound
    	){

    	IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();
    	// both are null - no change
    	if(oldLowerHashBound == null && newLowerHashBound == null){ 
	    	return false;
	    }
	    // both are non null and the same value - no change
    	if(oldLowerHashBound != null && newLowerHashBound != null && comp.compare(oldLowerHashBound.hash, newLowerHashBound.hash) == 0){
	    	return false;
    	}
    	// previously lowerHash == me, this server covered entire hash ring
		if(comp.compare(oldLowerHashBound.hash, me.hash) == 0){
			
			// a different node other than me is added
			if(comp.compare(newLowerHashBound.hash, me.hash) != 0){
				return true;
			}
		}
		else{

			// old below was between the center and me, need to see if the new one is greater than old and less than me
			if(comp.compare(oldBelow.hash, me.hash) < 0){
				if(comp.compare(newLowerHashBound.hash, oldLowerHashBound.hash) > 0 && comp.compare(newLowerHashBound.hash, me.hash) < 0){
					return true;
				}
			}
			else{ // old below was greater than me, i was responsible for above and below the center
				// see if the new is less than me or greater than olds
				if(comp.compare(newLowerHashBound.hash, oldLowerHashBound.hash) > 0 || comp.compare(newLowerHashBound.hash, me.hash) < 0){
					return true;
				}
			}
		}
		return false;
    }


    /**
	 *
	 * @param me : ConsistentHasher.ServerRecord using this servers params
	 * @param serverList : Array of ServerRecord in the updated hasher
	 *
	 * @return SimpleEntry<true, ...> if this server's ServerRecord is found in serverList, false otherwise
	 * @return SimpleEntry<...,  ServerRecord> The ServerRecord returned is the one representing the lower hash range. 
	 *											Is == me if there is only 1 entry in serverList
	 * 
	 * @return null if serverList is empty 
	*/
	public ConsistentHasher.ServerRecord getNewBelow( 
		ConsistentHasher.ServerRecord me, 
		ConsistentHasher.ServerRecord[] serverList ) 
	{

		IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();
		if(serverList.length < 1){
			return null;
		}

		if(serverList.length == 1){
			return serverList[0];
		}

		// at least 2, iterate through
		int i = 0;

		for(ConsistentHasher.ServerRecord rec : serverList){
			if(comp.compare(rec.hash, me.hash) >= 0){
				break;
			}
			i++;
		}
		if(i == serverList.length){
			// reached the end, the newBelow == [0]
			return serverList[0];
		}
		else if(i == 0){ // take the top array index (loop back around)
			return serverList[serverList.length - 1];
		}
		// take the previous
		return serverList[i - 1];
	}


    public boolean gettingNeighbourData = false;
	@Override
	public synchronized void consistentHasherUpdated(IConsistentHasher hasher) {
		// TODO Auto-generated method stub
		/*
			Check if the update means we will be sending some data
		*/
		String logHeader = name+":"+desired_port +" consistentHasherUpdated () ";

		System.out.println( logHeader + " - called");

		// run is called but start() has not been
		if(isStarted == false){ // nothing going on, the server is just listening but has not registered to the hash ring
			System.out.println(logHeader + " - isStarted==false");
			this.hasher = hasher;
			return;
		}

		ConsistentHasher.ServerRecord[] serverList = hasher.getServerList();

		this.hasher = hasher;

		if(serverList.length < 1){ // there were no hashed values updated, perhaps this could be some error? might want to check before clearStorage
			if(oldBelow != null){
				oldBelow = null;
				// clearStorage();
				return;
			}
		}

		IConsistentHasher.HashComparator comp = new IConsistentHasher.HashComparator();

		ConsistentHasher.ServerRecord me = new ConsistentHasher.ServerRecord(name, desired_port);

		boolean hashDelete = false;

		boolean hasMe = false;
		for(ConsistentHasher.ServerRecord rec : serverList){
			if(comp.compare(rec.hash, me.hash) == 0){
				hasMe = true;
				break;
			}
		}

		ConsistentHasher.ServerRecord newBelow = getNewBelow(me, serverList);

		if(oldBelow == null){ // we will not have been registered to cache ring yet
			
			if(!hasMe){ // still not registered, do nothing
				return;
			}
			if(serverList.length == 1){ // there is only 1 ServerRecord in new hasher and it is me, now registered to hash ring
				oldBelow = me;
				return;
			}
			// else length is > 1 and we are included for first time, check if we need to delete any overlapping hashed values (shouldn't though)
			hashDelete = true;
			oldBelow = me;
		}
		else{ // oldBelow != null, this mean we have previously been registered to the hash ring
			
			if(!hasMe){ // now unregistered from hash ring for some reason
				oldBelow = null;
				System.out.println(logHeader + " - unregistered from hash ring ");
				return;
			}
			// CHECK IF THE RING SHRUNK
			boolean ringShrunk = testRingShrunk(me, oldBelow, newBelow);
		
			if(ringShrunk){
				System.out.println(logHeader + " ring has shrunk, must delete data");
				hashDelete = true;
			}
		}
		if(hashDelete){

			if(comp.compare(newBelow.hash, me.hash) == 0){ // don't delete
				oldBelow = newBelow;
				return;
			}

			System.out.println(logHeader + " - hash delete ");
			byte[] oldBelowByte = new byte[oldBelow.hash.length];
			for (int j = 0; j < oldBelow.hash.length; j++) oldBelowByte[j] = oldBelow.hash[j];

			byte[] newBelowByte = new byte[newBelow.hash.length];
			for (int j = 0; j < newBelow.hash.length; j++) newBelowByte[j] = newBelow.hash[j];

			ArrayList<String> keysToDelete = new ArrayList<String>();
			Iterator<Map.Entry<String, String>> iter = cache.getHashRangeIterator(oldBelowByte, newBelowByte);
			Map.Entry<String,String> item = null;
			while(iter.hasNext()){
				keysToDelete.add(iter.next().getKey());
			}
			iter = null;

			for (String del_key : keysToDelete ) {
				try{
					cache.delete(del_key);	
				} catch (ICache.KeyDoesntExistException e){
					System.out.println(logHeader + " key doesnt exist");
				} catch(Exception e){
					System.out.println(logHeader + " exceptiom " + e.getMessage());
				}
			}
		}
		if(newBelow != null){
			oldBelow = newBelow;
		}

		return;
		
	}

	@Override
	public void OnTuplesReceived(Entry<?, ?>[] tuples) {
	

		System.out.println("On Tuples Received Called in " + name + ":" + desired_port + " And tuple size : " + tuples.length);
		for (int i = 0; i < tuples.length; ++i) {
			try{
				cache.put((String) tuples[i].getKey(), (String) tuples[i].getValue());
			} catch (Exception e){
				System.out.println("OnTuplesReceived exception " + e.getMessage());
			}
		}

		return;


		// // check if lock already had, otherwise get the lock
		// if(!isWriteLocked()){
		// 	lockWrite();
		// }
		// for (int i = 0; i < tuples.length; ++i) {
		// 	cache.put(tuples.getKey(), tuples.getValue());
		// }

		// // THIS WOULD REQUIRE THAT WE HAVE A CLIENT OUTPUT STREAM
		// KVMessage transferComplete = new KVMessage(StatusType.TRANSFER_COMPLETE, "", "");
		// KVMessage tcResp = bulkClient.SendMessage(transferComplete);

	}

	public DeleteTimeout deleteTimeoutRunnable = null;
	public Thread deleteTimeoutThread = null;

	public byte[] getHashedValue(byte[] bytes) throws RuntimeException {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] keyhash = md.digest(bytes);
			return keyhash;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage()); // fatal
		} catch (Exception e){

		}
		return null;
	}

	public UnlockTimeout unlockTimeoutRunnable = null;
	public Thread unlockTimeoutThread = null;

	@Override
	public void OnTuplesRequest(Byte[] lower, Byte[] upper, OutputStream client) {

		// check if lock already had, otherwise get the lock
		if(!lockWrite.testLockWrite()){
			// there was already a write lock!!! should probably either wait or throw exception here
		}
		
		// convert to byte[] for getHashRangeIterator
		byte[] lowerByte = new byte[lower.length];
		for (int i = 0; i < lower.length; i++) lowerByte[i] = lower[i];

		byte[] upperByte = new byte[upper.length];
		for (int i = 0; i < upper.length; i++) upperByte[i] = upper[i];
	    
        Iterator<Map.Entry<String, String>> iter = cache.getHashRangeIterator(lowerByte, upperByte);

        int maxPacketSize = 100000;
        
        while(iter.hasNext()){
        	ArrayList<Entry<?, ?>> tuples = new ArrayList<Entry<?, ?>>();
        	int count = 0;

        	int currentPacketSize = 0;

        	while(iter.hasNext() && currentPacketSize <= maxPacketSize){
	        	Map.Entry<String, String> currentEntry = iter.next();
	        	tuples.add(currentEntry);
	        	currentPacketSize += currentEntry.getKey().length() + currentEntry.getValue().length();
	        	count++;
	        }
	        try {
	        	BulkPackageMessage msg = new BulkPackageMessage((Entry<?, ?>[]) tuples.toArray());
	        	System.out.println("Listener writing response...");
				client.write(msg.getBytes());
	        } catch (Message.FormatException e){
	        	System.out.println("format Exception sending bulk package : " + e.getMessage());
	        } catch(IOException ee){
	        	System.out.println("io Exception sending bulk package : " + ee.getMessage());
	        }
        }

        try {
        	BulkPackageMessage finalMsg = new BulkPackageMessage(new Entry<?, ?>[0]);
	        client.write(finalMsg.getBytes());
        } catch (Message.FormatException e){
        	System.out.println("format Exception sending bulk package : " + e.getMessage());
        } catch(IOException ee){
        	System.out.println("io Exception sending bulk package : " + ee.getMessage());
        }

        

        // need to make sure it was successfull, then can delete those entries
        // this is where to add a timeout

        long timeoutMillis = 30000;
        unlockTimeoutRunnable = new UnlockTimeout(timeoutMillis, this.lockWrite);
        Thread timeoutUnlockThread = new Thread(unlockTimeoutRunnable);
        timeoutUnlockThread.start();

	}


	// interupts occur if either there is a timeout and lockWrite.unlockWrite() is called 
	// indicates timeout by calling unlockWrite() so external objects can use testUnlock()
	public class UnlockTimeout implements Runnable{

		long startTimeMillis;
		long totalWaitMillis;
		long endTimeMillis;

		LockWrite lockWrite;

		boolean interrupt;


		// can only add more time if the runnable is not yet finished (by interupt or timeout)
		public synchronized boolean addMoreTime(long timeToAddMillis) {
			if(!isRunning()){
				return false;
			}
			endTimeMillis += timeToAddMillis;
			return true;
		}


		public synchronized boolean isRunning(){

			if(System.currentTimeMillis() >= endTimeMillis || interrupt){
				return false;
			}
			return true;
		}

		// cannot use regular thread interupt as might interupt ?delete?
		// return true if interrupted
		// return false if already stopped
		public synchronized boolean interrupt(){ 
			if(!isRunning()){
				return false;
			}
			interrupt = true;	
			return true;
		}

		public synchronized boolean runLoopIteration(){
			if( interrupt ){
				return false;
			}
			else if(!lockWrite.isLockWrite()){
				interrupt = true; // used as a flag for complete
				return false;
			}
			else if(System.currentTimeMillis() >= endTimeMillis){
				// timed out before interupt
				lockWrite.testUnlockWrite();
				interrupt = true;
				return false;
			}
			return true;
		}
		public void run(){
			while(true){
				if(!runLoopIteration()){ // either timeout or detect interupt
					return;
				}
				try{
					Thread.sleep(10);
				} catch(InterruptedException e){

				}
				
			}
		}


		public UnlockTimeout(long totalWaitMillis, LockWrite lockWrite){
			startTimeMillis = System.currentTimeMillis();
			this.totalWaitMillis = totalWaitMillis;
			endTimeMillis = startTimeMillis + totalWaitMillis;
			this.lockWrite = lockWrite;

			interrupt = false;
		}
	}




	// When a node is receiving data from a server going down and the receiver times out it will have to remove any data already received
	// needs to indicate that unlockWrite was done in timeout 
	// DeleteTimeout.interupt() needs to occur to prevent it from timing out and deleting
	public class DeleteTimeout implements Runnable {
		HashComparator comp = new HashComparator();

		long startTimeMillis;
		long totalWaitMillis;
		long endTimeMillis;
		boolean interrupt = false;

		Object interruptLockObj = new Object();

		boolean isTimeout = false;

		ICache cache;
		Byte[] upperHash;
		Byte[] lowerHash;

		LockWrite lockWrite;


		public synchronized void updateMinHashVal(Byte[] newMinHash){

			ConsistentHasher.ServerRecord min = new ConsistentHasher.ServerRecord(newMinHash);
			ConsistentHasher.ServerRecord low = new ConsistentHasher.ServerRecord(lowerHash);

			if(comp.compare(min.hash, low.hash) < 0){
				lowerHash = newMinHash;
			}
		}


		public synchronized void updateMaxHashVal(Byte[] newMaxHash){

			ConsistentHasher.ServerRecord max = new ConsistentHasher.ServerRecord(newMaxHash);
			ConsistentHasher.ServerRecord upper = new ConsistentHasher.ServerRecord(upperHash);

			if(comp.compare(max.hash, upper.hash) > 0){
				upperHash = newMaxHash;
			}
		}

		public synchronized boolean addMoreTime(long timeToAddMillis) {
			if(isTimeout()){
				return false;
			}
			endTimeMillis += timeToAddMillis;
			return true;
		}

		// synchronize to more time can be added synchronously, if the thread is not is not running then is already timeout
		public synchronized boolean isTimeout(){
			return (System.currentTimeMillis() >= endTimeMillis);
		}

		public boolean interrupt(){ // cant use regular interupt because it will halt execution possibly during delete
			if(isTimeout()){
				return false;
			}
			synchronized(interruptLockObj){
				interrupt = true;
			}
			return true;
		}


		public synchronized boolean runLoopIteration(){
			if( interrupt ){
				return false;
			}
			else if(System.currentTimeMillis() >= endTimeMillis){
				// timed out before interupt

				byte[] upperByte = new byte[upperHash.length];
				for (int i = 0; i < upperHash.length; i++) upperByte[i] = upperHash[i];

				byte[] lowerByte = new byte[lowerHash.length];
				for (int i = 0; i < lowerHash.length; i++) lowerByte[i] = lowerHash[i];


				Iterator<Map.Entry<String, String>> iter = cache.getHashRangeIterator(lowerByte, upperByte);
				Map.Entry<String,String> item = null;
				while((item = iter.next()) != null){
					try{
						cache.delete(item.getKey());
					} catch(ICache.KeyDoesntExistException e){

					} catch(Exception e){

					}
					
				}


				interrupt = true;
				return false;
			}
			return true;
		}

		public void deleteFromCache(){

			byte[] upperByte = new byte[upperHash.length];
			for (int i = 0; i < upperHash.length; i++) upperByte[i] = upperHash[i];

			byte[] lowerByte = new byte[lowerHash.length];
			for (int i = 0; i < lowerHash.length; i++) lowerByte[i] = lowerHash[i];

			Iterator<Map.Entry<String, String>> iter = cache.getHashRangeIterator(lowerByte, upperByte);
			Map.Entry<String,String> item = null;
			while((item = iter.next()) != null){
				try{
					cache.delete(item.getKey());
				} catch (ICache.KeyDoesntExistException e){

				} catch(Exception e){

				}
				
			}
		}

		public void run(){
			
			while(true){
				if(!runLoopIteration()){ // either timeout or detect interupt
					return;
				}
				try{
					Thread.sleep(10);
				} catch (InterruptedException e){

				}
				
			}
		}

		public DeleteTimeout(long totalWaitMillis, LockWrite lockWrite, Byte[] upperHash, Byte[] lowerHash, ICache cache){
			startTimeMillis = System.currentTimeMillis();
			this.totalWaitMillis = totalWaitMillis;
			endTimeMillis = startTimeMillis + totalWaitMillis;
			this.lockWrite = lockWrite;
			this.cache = cache;
			this.upperHash = upperHash;
			this.lowerHash = lowerHash;
		}
	}

	
	// Node comming up:

	// 	Sends the initial request for data (as client)
	// 		- Existing node will lock and send data packets
	// 		- unlockWrite() called after some timeout, unless TRANSFER_COMPLETE received by server

	// 	When done loading, the new node sends TRANSFER_COMPLETE
	// 		- if TRANSFER_COMPLETE_ACK is received then we ics.addServer(me)
	// 		- else if TRANSFER_COMPLETE_NACK is received then failed, do not add server


	// Node going down:
	// 	Sends all the tuples (as client)
	// 		- Node going down writeLock and sets timeout to unlockWrite()
	// 		- Other node (server) sends back TRANSFER_COMPLETE
	// 	When TRANSFER_COMPLETE received, node going down checks if timedout
	// 		- If no timeout yet: reply TRANSFER_COMPLETE_ACK, remove from zookeeper
	// 		- If already timed out, send TRANSFER_COMPLETE_NACK
	

	

	 


	// THESE FUNCTIONS ARE ONLY CALLED AT SERVER END
	// sent back to us after a new node has loaded all the data we sent for its hash
	public void OnTransferCompleteReceived(KVMessage msg, OutputStream client) {
		return;
	}

	// we have received a node going down's data, and have sent back TRANSFER_COMPLETE
	// receiving TRANSFER_COMPLETE_ACK means that the node will complete going down
	public void OnTransferCompleteACKReceived(KVMessage msg, OutputStream client) {
		// we will just keep the data, eventually zookeeper will tell us its been removed

		if(!deleteTimeoutRunnable.interrupt()){

		}
	}

	// we have received a node going down's data, and have sent back TRANSFER_COMPLETE
	// receiving TRANSFER_COMPLETE_NACK means that the node timed out and will not be going down
	public void OnTransferCompleteNACKReceived(KVMessage msg, OutputStream client) {

		// we will delete the data received, zookeeper will not be updating us
		if(deleteTimeoutRunnable.interrupt()){
			deleteTimeoutRunnable.deleteFromCache();
			deleteTimeoutRunnable = null;
		}
		else{
			// was already interupted probably
		}
	}

}
