package app_kvECS;

import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;
import app_kvECS.ISSHLauncher;

public class ECSClient implements IECSClient {

	// private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    
    private boolean stop = false;
    
	private String serverAddress;
	private int serverPort;

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
			}
		}
    }

    private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		// if(tokens[0].equals("quit")) {	
		// 	stop = true;
		// 	disconnect();
		// 	System.out.println(PROMPT + "Application exit!");
		
		// } else if (tokens[0].equals("connect")){
		// 	if(tokens.length == 3) {
		// 		try{
		// 			serverAddress = tokens[1];
		// 			serverPort = Integer.parseInt(tokens[2]);
		// 			newConnection(serverAddress, serverPort);
		// 		} catch(NumberFormatException nfe) {
		// 			printError("No valid address. Port must be a number!");
		// 			logger.info("Unable to parse argument <port>", nfe);
		// 		} catch (UnknownHostException e) {
		// 			printError("Unknown Host!");
		// 			logger.info("Unknown Host!", e);
		// 		} catch (IOException e) {
		// 			printError("Could not establish connection!");
		// 			logger.warn("Could not establish connection!", e);
		// 		} catch (Exception e) {
        //             // e.getMessage();
		// 			printError(e.getMessage());
        //         }
		// 	} else {
		// 		printError("Invalid number of parameters!");
		// 	}

		// } else if(tokens[0].equals("disconnect")) {
		// 	disconnect();
			
		// } else if(tokens[0].equals("logLevel")) {
		// 	if(tokens.length == 2) {
		// 		String level = setLevel(tokens[1]);
		// 		if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
		// 			printError("No valid log level!");
		// 			printPossibleLogLevels();
		// 		} else {
		// 			System.out.println(PROMPT + 
		// 					"Log level changed to level " + level);
		// 		}
		// 	} else {
		// 		printError("Invalid number of parameters!");
		// 	}
			
		// } else if(tokens[0].equals("help")) {
		// 	printHelp();
		// } else if(tokens[0].equals("put")) {
		// 	if(tokens.length >= 2) {
		// 		if(client != null){
		// 			try {
		// 				String key = tokens[1];
		// 				StringBuilder msg = new StringBuilder();
		// 				for(int i = 2; i < tokens.length; i++) {
		// 					msg.append(tokens[i]);
		// 					if (i != tokens.length -1 ) {
		// 						msg.append(" ");
		// 					}
		// 				}
		// 				KVMessage kvmsg = client.put(key, msg.toString());
		// 				StatusType statusType = kvmsg.getStatus();
						
		// 				if(statusType == StatusType.PUT_SUCCESS) {
		// 					System.out.println(PROMPT + "Put " + key + " succeeded.");
		// 					System.out.println(PROMPT + "Value: " + msg.toString());
		// 				} else if(statusType == StatusType.PUT_ERROR) {
		// 					printError("Put failed.");
		// 				} else if(statusType == StatusType.PUT_UPDATE) {
		// 					System.out.println(PROMPT + "Update " + key + " succeeded.");
		// 					System.out.println(PROMPT + "Value: " + msg.toString());
		// 				} else if(statusType == StatusType.DELETE_SUCCESS) {
		// 					System.out.println(PROMPT + "Delete " + key + " succeeded.");
		// 				} else if(statusType == StatusType.DELETE_ERROR) {
		// 					printError("Delete failed.");
		// 				} else {
		// 					printError("Nothing happened.");
		// 				}
		// 			} catch (Exception e) {
		// 				printError(e.getMessage());
		// 			}
		// 		} else {
		// 			printError("Not connected!");
		// 		}
		// 	} else {
		// 		printError("Invalid number of parameters!");
		// 	}
		// } else if(tokens[0].equals("get")) {
		// 	if(tokens.length == 2) {
		// 		if(client != null){
		// 			try {
		// 				String key = tokens[1];
		// 				KVMessage kvmsg = client.get(key);
		// 				StatusType statusType = kvmsg.getStatus();

		// 				if(statusType == StatusType.GET_SUCCESS) {
		// 					System.out.println(PROMPT + "Get " + key + " succeeded.");
		// 					System.out.println(PROMPT + "Value: " + kvmsg.getValue());
		// 				} else if(statusType == StatusType.GET_ERROR) {
		// 					printError("Get failed.");
		// 				} else {
		// 					printError("Nothing happened.");
		// 				}
		// 			} catch (Exception e) {
		// 					printError("Get failed.");
		// 					// printError(e.getMessage());
		// 			}
		// 		} else {
		// 			printError("Not connected!");
		// 		}
		// 	} else {
		// 		printError("Invalid number of parameters!");
		// 	}
		// }else {
		// 	printError("Unknown command");
		// 	printHelp();
		// }
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
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
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
        try {
            // new LogSetup("logs/client/client.log", Level.OFF);
            ECSClient ecsclient = new ECSClient();
            ecsclient.start();
        } catch (Exception e) {
            e.printStackTrace();
			System.exit(1);
        }
    }
}
