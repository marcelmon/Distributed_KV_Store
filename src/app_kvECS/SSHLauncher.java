package app_kvECS;

import SSHScript.sh;

public class SSHLaucher implements ISSHLauncher {
    // lauch a SSH connection
    public boolean launchSSH() {
        Process proc;
        String script = "SSHScript.sh";
        
        Runtime run = 	Runtime.getRuntime();
        try {
            proc = run.exec(script);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return true;
    }
}