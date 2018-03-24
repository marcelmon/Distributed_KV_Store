package app_kvECS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SSHLauncher implements ISSHLauncher {
	protected List<Process> processes = new ArrayList<Process>();
	
    // lauch a SSH connection
    public boolean launchSSH(String name, String hostname, int port, int cachesize, String cachestrategy, String zkHost, int zkPort) throws Exception {
        Process proc;
        Runtime run = Runtime.getRuntime();
        try {
        	// From SSH command:
        	String cmd = 
        			"ssh -n " + hostname + " " +
//        			"\"java -jar m2-server.jar " + 
					"java -jar m2-server.jar " +
        			name + " " + 
        			port + " " +
        			zkHost + " " +
    				zkPort + " " +
        			cachesize + " " +
        			cachestrategy + 
//        			" & echo \\$!\" &";
        			" &";
        	System.out.println(cmd);
        	
        	// Execute command:
            proc = run.exec(cmd);
            processes.add(proc);
            
            // Get the output from SSH:
//            Thread.sleep(1000);
            proc.waitFor(1000, TimeUnit.MILLISECONDS);
            InputStreamReader isr = new InputStreamReader(proc.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            int c;
            String output = "";
            while (br.ready()) {
//                System.out.print((char)c);
            	c = br.read();
            	output += (char) c;
            }
            
            System.out.println(output);
//            // The first word is the pid of the process:
//            String pid = output.split(" ")[0];
//            System.out.println("pid: " + pid);
            
        } catch (IOException e) {
            proc = null;
            throw new Exception(e.getMessage());
        }

        return true;
    }
    
    public void killall() {
    	//FIXME implement properly
    	for (Process p : processes) {
    		p.destroy();    		
    	}
    	processes.clear();
    }
}
