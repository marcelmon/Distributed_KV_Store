package app_kvECS;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.util.Map;
import java.util.Collection;
import java.util.Scanner;
import java.util.ArrayList;

import ecs.*;
import app_kvECS.ISSHLauncher;
import common.comms.IIntraServerComms.RPCMethod;
import common.comms.*;

public class ECSClient implements IECSClient {

	// private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    
    private boolean stop = false;
	
	// non-static variable ecs_config cannot be referenced from a static context (error prompt without static, WHYYY???)
	// private static String[] ecs_config = new String[8]; // assume there is 8 servers in the ecs.config

	// non-static variable ecs_config cannot be referenced from a static context (error prompt without static, WHYYY???)
	private static ArrayList<String> ecs_config = new ArrayList<String>();
	private int nxtsertoadd = 0; // next server to be added
	private int nxtnodetoadd = 0; // next node to be added

	private Collection<IECSNode> addedNode = new ArrayList<IECSNode>();

    @Override
    public boolean start() throws Exception {
        // TODO
        while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
                printError("CLI does not respond - Application terminated ");
                return false;
			}
        }
        return true;
    }

    private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("shutdown")) {	
			stop = true;
			shutdown();
			System.out.println(PROMPT + "Application shutdown!");
		
		} else if (tokens[0].equals("stop")) {
			stop();
			System.out.println(PROMPT + "Application stop!");
		} else if (tokens[0].equals("addNode")) {
			if(tokens.length == 3) {
				IECSNode temp;
				temp = addNode(tokens[1], Integer.parseInt(tokens[2]));
				addedNode.add(temp);
				System.out.println(PROMPT + "Node Added!");
			} else {
				printError("Invalid number of parameters!");
			}
		} else if (tokens[0].equals("addNodes")) {
			if(tokens.length == 4) {
				Collection<IECSNode> temp = new ArrayList<IECSNode>();
				temp = addNodes(Integer.parseInt(tokens[1]),tokens[2], Integer.parseInt(tokens[3]));
				addedNode.addAll(temp);
				System.out.println(PROMPT + "Nodes Added!");
			} else {
				printError("Invalid number of parameters!");
			}
		} else {
			printError("Unknown command");
			printHelp();
		}
    }
    
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECSCLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("add node <cache strategy> <cache size>");
		sb.append("\t\t Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.\n");
		sb.append(PROMPT).append("add nodes <count> <cache strategy> <cache size>");
		// sb.append("\t\t inserts a key-value pair into the storage server \n");
		// sb.append(PROMPT).append("get <key>");
		// sb.append("\t\t\t retrives the value for the given key from the storage server \n");
		sb.append(PROMPT).append("stop");
		sb.append("\t\t\t Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running. \n");
		sb.append(PROMPT).append("shutdown");
		sb.append("\t\t\t Stops all server instances and exits the remote processes. \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t\t exits the program");
		System.out.println(sb.toString());
	}
    
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
    }
    
    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
		// TODO
		Collection<IECSNode> temp =addNodes(1, cacheStrategy, cacheSize);
		return temp.iterator().next();
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
		// TODO
		
		// instead of randomly, just open first "count"-numbers of servers.
		for (int i = 0; i < count; i++) {
			if (nxtsertoadd >= ecs_config.size()) {
				System.out.println("Error: Ran out of services.");
				return null;
			}
			String[] tokens = ecs_config.get(nxtsertoadd).split("\\s+");
			if (tokens.length > 3) {
				System.out.println("Error: ecs.config error <name> <hostname> <port>");
				return null;
			}

			try {
				SSHLauncher SSH_server = new SSHLauncher();
				SSH_server.launchSSH(tokens[1],Integer.parseInt(tokens[2]));

				IntraServerComms comm = new IntraServerComms(tokens[0],tokens[1],Integer.parseInt(tokens[2]));
				comm.call(tokens[1] + ":" + tokens[2],RPCMethod.Start,"");
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

			nxtsertoadd++;
		}

		return setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
		// TODO
		Collection<IECSNode> result = new ArrayList<IECSNode>();
		
		for (int i = 0; i < count; i++) {
			String[] tokens = ecs_config.get(nxtnodetoadd).split("\\s+");

			IECSNode node = new ECSNode(tokens[0],tokens[1],Integer.parseInt(tokens[2]));
			result.add(node);

			nxtnodetoadd++;
		}
        return result;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
		// TODO
		while (!nodeNames.isEmpty()) {
			String temp = nodeNames.iterator().next();
			for (IECSNode i: addedNode) {
				if (temp.equals(i.getNodeName())) {
					try {
						IntraServerComms comm = new IntraServerComms(i.getNodeName(),i.getNodeHost(),i.getNodePort());
						comm.call(i.getNodeHost() + ":" + Integer.toString(i.getNodePort()),RPCMethod.Stop,"");
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}

					addedNode.remove(i);
					break;
				}
			}
			nodeNames.remove(temp);
		}

        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        // TODO
        // SSHLauncher SSH_server = new SSHLauncher();
        // SSH_server.launchSSH();
        if (args.length != 2) {
			System.exit(1);
        }

        try {
			Scanner s = new Scanner(new File(args[1]));
			while (s.hasNextLine()) {
				ecs_config.add(s.nextLine());
			}

            // new LogSetup("logs/client/client.log", Level.OFF);
            ECSClient ecsclient = new ECSClient();
            ecsclient.start();
        } catch (Exception e) {
            e.printStackTrace();
			System.exit(1);
        }
    }
}
