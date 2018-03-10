package app_kvECS;

import java.io.IOException;

public class SSHLauncher implements ISSHLauncher {
    // lauch a SSH connection
    public boolean launchSSH() throws Exception {
        Process proc;
        String script = "SSHScript.sh";
        
        Runtime run = Runtime.getRuntime();
        try {
            proc = run.exec(script);
        } catch (IOException e) {
            proc = null;
            throw new Exception(e.getMessage());
        }

        return true;
    }
}