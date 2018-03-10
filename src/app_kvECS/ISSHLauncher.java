package app_kvECS;

public interface ISSHLauncher {
    // lauch a SSH connection
    public boolean launchSSH(String hostname, int port) throws Exception;
}