package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import common.comms.ICommMod;
import common.comms.CommMod;
import common.messages.KVMessage;
import common.messages.TLVMessage;
import common.messages.KVMessage.StatusType;

public class KVClient implements IKVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private ICommMod client = null;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	public void run() {
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

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					newConnection(serverAddress, serverPort);
				// } catch(NumberFormatException nfe) {
				// 	printError("No valid address. Port must be a number!");
				// 	logger.info("Unable to parse argument <port>", nfe);
				// } catch (UnknownHostException e) {
				// 	printError("Unknown Host!");
				// 	logger.info("Unknown Host!", e);
				// } catch (IOException e) {
				// 	printError("Could not establish connection!");
				// 	logger.warn("Could not establish connection!", e);
				} catch (Exception e) {
                    e.getMessage();
                }
			} else {
				printError("Invalid number of parameters!");
			}

		} else if(tokens[0].equals("disconnect")) {
			disconnect();
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else if(tokens[0].equals("put")) {
			if(tokens.length >= 2) {
				if(client != null){
					try {
						String key = tokens[1];
						StringBuilder msg = new StringBuilder();
						for(int i = 2; i < tokens.length; i++) {
							msg.append(tokens[i]);
							if (i != tokens.length -1 ) {
								msg.append(" ");
							}
						}
						StatusType statusType = StatusType.PUT;
						KVMessage kvmsg = new TLVMessage(statusType,key,msg.toString());
						client.SendMessage(kvmsg);

// after send message, it stopped.

						if(statusType == StatusType.PUT_SUCCESS) {
							System.out.println(PROMPT + "Put " + key + " succeeded.");
						} else if(statusType == StatusType.PUT_ERROR) {
							printError("Put failed.");
						} else if(statusType == StatusType.PUT_UPDATE) {
							System.out.println(PROMPT + "Update " + key + " succeeded.");
						} else if(statusType == StatusType.DELETE_SUCCESS) {
							System.out.println(PROMPT + "Delete " + key + " succeeded.");
						} else if(statusType == StatusType.DELETE_ERROR) {
							printError("Delete failed.");
						} else {
							System.out.println(PROMPT + "Nothing happened.");
						}
					} catch (Exception e) {
						e.getMessage();
					}
				} else {
					printError("Not connected!");
				}
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
		sb.append(PROMPT).append("CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t inserts a key-value pair into the storage server \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t\t retrives the value for the given key from the storage server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
	private void disconnect() {
		if(client != null) {
			client.Disconnect();
			client = null;
		}
	}
	    
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        client = new CommMod();
        client.Connect(hostname, port);
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client/client.log", Level.OFF);
            KVClient app = new KVClient();
            app.run();
        } catch (Exception e) {
            System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
        }
    }
}
