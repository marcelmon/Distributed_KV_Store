package app_kvServer;

public interface IServerLauncher {
	/**
	 * Uses SSH to launch an instance of IKVServer with the following parameters.
	 * @param name The unique name of the server.
	 * @param hostname The host name or IP address of the server.
	 * @param port The port the server should listen on
	 * @param zkHostname The host name or IP address of the Zookeeper server.
	 * @param zkPort The port of the Zookeeper server
	 * @param cacheSize The size of the cache.
	 * @param strategy The caching strategy to be used
	 * @return true if the launch is successful, or false if there is an error
	 */
	public boolean launch(
			String name, 
			String hostname, 
			int port, 
			String zkHostname, 
			int zkPort, 
			int cacheSize, 
			String strategy);
}
