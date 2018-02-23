package common.comms;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ConsistentHasher implements IConsistentHasher {
	protected List<ServerRecord> servers = new ArrayList<ServerRecord>();

	@Override
	public void fromString(String metadata) throws StringFormatException {
		// string format is csv: "<ip/hostname>:<port>,..."
		servers.clear();
		
		String[] inputServers = metadata.split(",");		
		for (String server : inputServers) {
			// Attempt to split into host:port
			String[] sp_server = server.split(":");
			if (sp_server.length != 2) {
				throw new StringFormatException("Exactly one colon (:) should be present per server");
			}
			String hostname = sp_server[0];
			Integer port = null;
			try {
				port = Integer.parseInt(sp_server[1]);
			} catch (NumberFormatException e) {
				throw new StringFormatException("Couldn't parse a port as an integer (" + sp_server[1] + ")");
			}
			
			servers.add(new ServerRecord(hostname, port));
		}
	}
	
	@Override
	public String toString() {
		String output = "";
		for (ServerRecord server : servers) {
			output = output + "," + server.toString(); 
		}
		if (output.length() != 0) {
			return output.substring(1); // remove leading comma
		} else {
			return output;
		}
	}

	@Override
	public void fromServerList(List<ServerRecord> servers) {	
		this.servers = servers; 
		
		// Sort by hash:
		Collections.sort(this.servers, new HashComparator());
	}
	
	@Override
	public ServerRecord[] getServerList() {
		return servers.toArray(new ServerRecord[0]);
	}
	
	protected ServerRecord FindServer(ServerRecord query, boolean above) {
		HashComparator comp = new HashComparator();
		
		// See if it is below the bottom or above the top:
		if (comp.compare(query, servers.get(0)) < 0) {
			if (above) {
				return servers.get(0);
			} else {
				return servers.get(servers.size()-1);
			}
		}
		if (comp.compare(query, servers.get(servers.size()-1)) > 0) {
			if (above) {
				return servers.get(0);
			} else {
				return servers.get(servers.size()-1);
			}
		}
		
//		System.out.println("KEY HASH:" + keyhash[0]);
		
		// nb server list already sorted
		// just need to do a search for the server with hash just above the key
		int l = 0;
		int r = servers.size()-1;
		while (true) {
//			System.out.println("l: " + l + ", r: " + r);
			if (l == r) {
				break;
			}
			if (l == r-1) {
				break;
			}
			int c = (int) Math.ceil((l+r)/2); // round up so that [l, l+1] -> c=l+1
			
			// with every iteration, we are guaranteed that the key is within [l,r]
			// ultimately, we will either have [l, l+1] or [l, l] with the latter being the case
			// that the key hash equals one of the server hashes (e.g. the key is a server name:port)
			
			int comparison = comp.compare(query, servers.get(c)); 
			if (comparison < 0) {			// key < c
				if (r == c) {
					throw new RuntimeException("Infintie loop detected in mapKey() search"); // fatal
				}
				r = c;
			} else if (comparison > 0) {    // key > c
				if (l == c) {
					throw new RuntimeException("Infintie loop detected in mapKey() search"); // fatal
				}
				l = c;
			} else {						// key == c
				l = c;
				r = c;
			}
		}
		
		// Find correct mapping (next highest with circular wrapping)
		Integer nextHighest = null;

		// Four cases may occur:
		//   key is equal to lower: 
		//     if l==r, we want r+1                    								[A] 
		//     if r=l+1, we want r                     								[B]
		//   key is strictly inside: we want r		   								[C]
		//   key is equal to upper: we want r+1        								[D]
	    
		if (l == r || comp.compare(query,  servers.get(r)) == 0) {  // A or D
			nextHighest = r+1;
		} else {  // B or C
			nextHighest = r;
		}
		
		Integer tgt = nextHighest;
		if (!above) {
			tgt = nextHighest - 1;
		}
		if (tgt == servers.size()) {
			tgt = 0;
		} else if (tgt < 0) {
			tgt = servers.size() - 1;
		}
		
		return servers.get(tgt);
	}

	@Override
	public ServerRecord mapKey(String key) {
		if (servers.size() == 0) {
			return null;
		}
		
		//Hash the key
		Byte[] keyhash = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] primKeyhash = md.digest(key.getBytes());
			keyhash = new Byte[primKeyhash.length];
			for (int i = 0; i < primKeyhash.length; i++) {
				keyhash[i] = primKeyhash[i];
			}
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage()); // fatal
		}
		ServerRecord keyRecord = new ServerRecord(keyhash);
		
		return FindServer(keyRecord, true);
		
	}

	@Override
	public void preAddServer(ServerRecord me, ServerRecord txServer, List<Byte> newRangeLower, List<Byte> newRangeUpper) {
		// We want to find the server immediately ahead and return our hash and the hash of the server ahead
		newRangeLower.clear();
		newRangeUpper.clear();
		
		ServerRecord above = FindServer(me, true);
		ServerRecord below = FindServer(me, false);
		txServer.setEqualTo(above);
		
		newRangeLower.clear();
		newRangeLower.addAll(Arrays.asList(below.hash));
		newRangeUpper.clear();
		newRangeUpper.addAll(Arrays.asList(me.hash));
	}

	@Override
	public void preRemoveServer(ServerRecord me, ServerRecord rxServer) {
		ServerRecord above = FindServer(me, true);
		rxServer.setEqualTo(above);
	}

}
