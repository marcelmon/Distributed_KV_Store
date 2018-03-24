package app_kvECS;

public interface ISSHLauncher {
    // lauch a SSH connection
    public boolean launchSSH(String name, String hostname, int port, int cachesize, String cachestrategy, String zkHost, int zkPort) throws Exception;
    public void killall();
}
