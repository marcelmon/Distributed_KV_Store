package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.comms.CommListener;
import common.comms.CommMod;
import common.comms.ICommListener;

public class KVServer implements IKVServer {

	private static Logger logger = Logger.getRootLogger();
	
	private int port;
	private String hostname;
	private CacheStrategy cacheStrategy;
	private CommMod server = null;
	private boolean running;
	private ICache cache = null;
	private ICommListener listener = null;
	
	public KVServer(int port){
		this.port = port;
	}
	
	public void run() {
		
		// cache = new ICache();
		running = initializeServer();

		// logger.info("Server stopped.");
	}

	private boolean isRunning() {
		return this.running;
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

  private boolean initializeServer() {
   	logger.info("Initialize server ...");
   	try {
		server = new CommMod();
		listener = new CommListener();
		server.SetListener(listener);
		server.StartServer(port);
        logger.info("Server listening on port: " 
    	   		+ port);    
        return true;
    } catch (Exception e) {
       	logger.error("Error! Cannot open server socket:");
        if(e instanceof BindException){
           	logger.error("Port " + port + " is already bound!");
        }
        return false;
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
	}

	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return hostname;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return cacheStrategy;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return cache.getCacheSize();
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return cache.inStorage(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return cache.get(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		cache.put(key, value);
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
		cache.clearCache();
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		// cache.clearStorage();
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		cache = null;
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		cache = null;
	}

	public static void main (String[] args) {
		try {
			new LogSetup("logs/server/server.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				new KVServer(port).run();
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

}
