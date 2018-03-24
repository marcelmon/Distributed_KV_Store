package app_kvECS;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.util.Map;
import java.util.Collection;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.ArrayList;

import ecs.*;
import app_kvECS.ISSHLauncher;
import common.comms.IIntraServerComms.RPCMethod;
import common.comms.*;

public class AdminClient  {
	private static final String PROMPT = "AdminClient> ";
	private static ArrayList<String> ecs_config = new ArrayList<String>();		
	private SSHLauncher launcher = new SSHLauncher();
	private IntraServerComms isc;
	
    public void handleCommand(String cmdLine) throws Exception {
		String[] tokens = cmdLine.split("\\s+");
		
		if(tokens[0].equals("init")) {
			if (tokens.length != 3) {
				printError("Incorrect number of args!");
				printHelp();
				return;
			}
			if (isc != null) {
				printError("Already initialized!");
				return;
			}
			init(tokens[1], Integer.parseInt(tokens[2]));
			
		} else if(tokens[0].equals("startone")) {
			if (tokens.length != 2) {
				printError("Incorrect number of args!");
				printHelp();
				return;
			}
			startone(tokens[1]);
			
		} else if(tokens[0].equals("startall")) {	
			startall();
			
		} else if(tokens[0].equals("stopone")) {
			if (tokens.length != 2) {
				printError("Incorrect number of args!");
				printHelp();
				return;
			}
			stopone(tokens[1]);
		
		} else if (tokens[0].equals("stopall")) {
			stopall();
		
		} else if (tokens[0].equals("quit")) {
			System.exit(0);
		
		} else if (tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
    }
    
	private static void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECSCLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("init <zkHost> <zkPort>\n");
		sb.append(PROMPT).append("startone <host>:<port>\n");
		sb.append(PROMPT).append("startall\n");
		sb.append(PROMPT).append("stopone <host>:<port>\n");
		sb.append(PROMPT).append("stopall\n");
		sb.append(PROMPT).append("quit\n");
		System.out.println(sb.toString());
	}
    
	private static void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
    }
    
	private void init(String kvHost, int kvPort) throws Exception {
		// Launch all the servers:    	
    	for (String s : ecs_config) {
    		String[] tokens = s.split(" ");
    		if (tokens.length != 3) {
    			throw new Exception("Malformed server config");
    		}
    		System.out.println("Launching... " + s);
    		isc = new IntraServerComms(kvHost + ":" + kvPort, "ECS", 0);
    		launcher.launchSSH(tokens[0], tokens[1], Integer.parseInt(tokens[2]), 10, "LRU", kvHost, kvPort); //TODO magic values
    	}
	}
	
	private void startone(String server) throws Exception {
		if (isc == null) {
			printError("Need to init");
			return;
		}
		isc.call(server, RPCMethod.Start);
	}
	
	private void startall() throws Exception {
		for (String s : ecs_config) {
			String[] tokens = s.split(" ");
    		if (tokens.length != 3) {
    			throw new Exception("Malformed server config");
    		}
			startone(tokens[1] + ":" + tokens[2]);
		}
	}
	
	private void stopone(String server) throws Exception {
		if (isc == null) {
			printError("Need to init");
			return;
		}
		isc.call(server, RPCMethod.Stop);
	}
	
	private void stopall() throws Exception {
		for (String s : ecs_config) {
			String[] tokens = s.split(" ");
    		if (tokens.length != 3) {
    			throw new Exception("Malformed server config");
    		}
			stopone(tokens[1] + ":" + tokens[2]);
		}
	}
	
	public static void printUsage(String[] args) {
    	System.out.println("USAGE: m2-admin.jar <config file>");
    }
	
	public static void main(String[] args) {
        if (args.length != 1) {        	
        	printUsage(args);
			System.exit(1);
        }

        try {
			Scanner s = new Scanner(new File(args[0]));
			while (s.hasNextLine()) {
				ecs_config.add(s.nextLine());
			}
			s.close();

            // new LogSetup("logs/client/client.log", Level.OFF);
            AdminClient cli = new AdminClient();
            boolean stop = false;
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            while(!stop) {
    			System.out.print(PROMPT);
    			try {
    				String cmdLine = stdin.readLine();
    				cli.handleCommand(cmdLine);
    			} catch (IOException e) {
    				stop = true;
                    printError("CLI does not respond - Application terminated ");
    			} catch (Exception e) {
    				printError(e.getMessage());
    			}
            }
        } catch (Exception e) {
            e.printStackTrace();
			System.exit(1);
        }
    }
}
