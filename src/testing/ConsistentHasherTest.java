package testing;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import common.comms.*;
import common.comms.IConsistentHasher.*;
import junit.framework.TestCase;

public class ConsistentHasherTest extends TestCase {
	@Test
	public void testFromString() throws Exception {
		String metadata = "google.com:1234,amazon.com:5678";
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromString(metadata);
		ServerRecord[] serverList = hasher.getServerList();
		assertTrue(serverList[0].hostname.equals("google.com"));
		assertTrue(serverList[0].port.equals(1234));
		assertTrue(serverList[1].hostname.equals("amazon.com"));
		assertTrue(serverList[1].port.equals(5678));
	}
	
	@Test
	public void testHashingAndSorting() throws Exception {
		String A = "shdsjdsj";
		String B = "ddsidudjd";
		String C = "i2kss9";
		Integer p = 3000;
		
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord(C, p));
		serverList.add(new ServerRecord(B, p));
		serverList.add(new ServerRecord(A, p));

		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] primHashA = md.digest((A + ":" + p).getBytes());
		byte[] primHashB = md.digest((B + ":" + p).getBytes());
		byte[] primHashC = md.digest((C + ":" + p).getBytes());
		Byte[] hashA = new Byte[primHashA.length];
		Byte[] hashB = new Byte[primHashB.length];
		Byte[] hashC = new Byte[primHashC.length];
		for (int i = 0; i < primHashA.length; i++) {
			hashA[i] = primHashA[i];
		}
		for (int i = 0; i < primHashB.length; i++) {
			hashB[i] = primHashB[i];
		}
		for (int i = 0; i < primHashC.length; i++) {
			hashC[i] = primHashC[i];
		}
		
		assertTrue(Arrays.equals(hashA, new Byte[] {-91, 25, -86, 99, -111, -11, 12, 21, -99, -64, 66, -15, -11, 67, 124, 122}));
		assertTrue(Arrays.equals(hashB, new Byte[] {3, -60, -32, -95, 48, -69, -52, 32, -76, -25, 61, 61, -53, 61, 88, -24}));
		assertTrue(Arrays.equals(hashC, new Byte[] {76, -28, -86, 94, 101, -39, -122, 16, 116, -21, -60, 67, -70, -43, -18, 127}));
		
		// nb: order is A, B, C
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		ServerRecord[] serverListOut = hasher.getServerList();
		
		assertTrue(Arrays.equals(serverListOut[0].hash, hashA));
		assertTrue(Arrays.equals(serverListOut[1].hash, hashB));
		assertTrue(Arrays.equals(serverListOut[2].hash, hashC));
	}
	
	@Test
	public void testSorting() throws Exception {
		// Server list in reverse order (by hash):
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("amazon.com", 5678)); // hash[0]=-4
		serverList.add(new ServerRecord("google.com", 1234));  // hash[0]=-58
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] outputList = hasher.getServerList();
		
		assertTrue(outputList[0].hostname.equals("google.com"));
		assertTrue(outputList[1].hostname.equals("amazon.com"));
	}
	
	@Test
	public void testToString() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("google.com", 1234));
		serverList.add(new ServerRecord("amazon.com", 5678));
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		String metadata = hasher.toString();
		String metadataExpected = "google.com:1234,amazon.com:5678";
		
		assertTrue(metadata.equals(metadataExpected));
	}
	
	@Test
	public void testMapKey() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] serverListOut = hasher.getServerList();
//		for (ServerRecord rec : serverListOut) {
//			System.out.print(rec.hostname + ":");
//			for (int i = 0; i < rec.hash.length; i++) {
//				System.out.print(rec.hash[i] + ",");
//			}
//			System.out.println("");
//		}
		assertTrue(serverListOut.length == 5);
		assertTrue(serverListOut[0].hostname.equals("1"));
		assertTrue(serverListOut[1].hostname.equals("3"));
		assertTrue(serverListOut[2].hostname.equals("4"));
		assertTrue(serverListOut[3].hostname.equals("2"));
		assertTrue(serverListOut[4].hostname.equals("0"));
		
		ServerRecord target = null;
		
		target = hasher.mapKey("mykey");
		assertTrue(target.hostname.equals("4"));
	}
	
	@Test
	public void testMapKeyRedundant() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		String query = "mykey";
		ServerRecord target = hasher.mapKey(query);
		List<ServerRecord> redundant = hasher.mapKeyRedundant(query, 2);
		
//		System.out.println("Target: " + target.hostname);
//		System.out.println("r0: " + redundant.get(0).hostname);
//		System.out.println("r1: " + redundant.get(1).hostname);
		
		// Ensure we have exactly two redundant servers:
		assertTrue(redundant.size() == 2);
		
		// Remove target and ensure we map to the first redundant server:
		assertTrue(serverList.remove(target));
		hasher.fromServerList(serverList);
		assertTrue(hasher.mapKey(query).hostname.equals(redundant.get(0).hostname));
		
		// Remove first redundant and ensure we map to the second redundant server:
		assertTrue(serverList.remove(redundant.get(0)));
		hasher.fromServerList(serverList);
		assertTrue(hasher.mapKey(query).hostname.equals(redundant.get(1).hostname));
	}
	
	@Test
	public void testMapKeyRedundantSmall() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		// order: 1, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		String query = "mykey";
		ServerRecord target = hasher.mapKey(query);
		List<ServerRecord> redundant = hasher.mapKeyRedundant(query, 2);
		
		// Ensure we have exactly one redundant servers (as we don't want overlap in the
		// target and redundant servers):
		assertTrue(redundant.size() == 1);
	}
	
	@Test
	public void testMapKeyEquals() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] serverListOut = hasher.getServerList();
//		for (ServerRecord rec : serverListOut) {
//			System.out.print(rec.hostname + ":");
//			for (int i = 0; i < rec.hash.length; i++) {
//				System.out.print(rec.hash[i] + ",");
//			}
//			System.out.println("");
//		}
		assertTrue(serverListOut.length == 5);
		assertTrue(serverListOut[0].hostname.equals("1"));
		assertTrue(serverListOut[1].hostname.equals("3"));
		assertTrue(serverListOut[2].hostname.equals("4"));
		assertTrue(serverListOut[3].hostname.equals("2"));
		assertTrue(serverListOut[4].hostname.equals("0"));
		
		ServerRecord target = null;
		
		target = hasher.mapKey("1" + ":" + 0);
//		System.out.println("1->" + target.hostname);
		assertTrue(target.hostname.equals("3"));
		
		target = hasher.mapKey("3" + ":" + 0);
//		System.out.println("3->" + target.hostname);
		assertTrue(target.hostname.equals("4"));
		
		target = hasher.mapKey("4" + ":" + 0);
//		System.out.println("4->" + target.hostname);
		assertTrue(target.hostname.equals("2"));
		
		target = hasher.mapKey("2" + ":" + 0);
//		System.out.println("2->" + target.hostname);
		assertTrue(target.hostname.equals("0"));
		
		target = hasher.mapKey("0" + ":" + 0);
//		System.out.println("0->" + target.hostname);
		assertTrue(target.hostname.equals("1"));
	}
	
	@Test
	public void testMapKeyTop() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] serverListOut = hasher.getServerList();
//		for (ServerRecord rec : serverListOut) {
//			System.out.print(rec.hostname + ":");
//			for (int i = 0; i < rec.hash.length; i++) {
//				System.out.print(rec.hash[i] + ",");
//			}
//			System.out.println("");
//		}
		assertTrue(serverListOut.length == 5);
		assertTrue(serverListOut[0].hostname.equals("1"));
		assertTrue(serverListOut[1].hostname.equals("3"));
		assertTrue(serverListOut[2].hostname.equals("4"));
		assertTrue(serverListOut[3].hostname.equals("2"));
		assertTrue(serverListOut[4].hostname.equals("0"));
		
		ServerRecord target = null;
		
//		Integer key = 0;
//		byte[] hash = null;
//		do {
//			key = key + 1;
//			MessageDigest md = MessageDigest.getInstance("MD5");
//			hash = md.digest(Integer.toString(key).getBytes());
//		} while (hash[0] <= serverListOut[4].hash[0]);
//		System.out.println("Key: " + key);
//		System.out.println("hash[0]: " + hash[0]);
		
		Integer key = 9;
		target = hasher.mapKey(Integer.toString(key));
//		System.out.println(target.hostname);
	
		assertTrue(target.hostname.equals("1"));
	}
	
	@Test
	public void testMapKeyBottom() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] serverListOut = hasher.getServerList();
//		for (ServerRecord rec : serverListOut) {
//			System.out.print(rec.hostname + ":");
//			for (int i = 0; i < rec.hash.length; i++) {
//				System.out.print(rec.hash[i] + ",");
//			}
//			System.out.println("");
//		}
		assertTrue(serverListOut.length == 5);
		assertTrue(serverListOut[0].hostname.equals("1"));
		assertTrue(serverListOut[1].hostname.equals("3"));
		assertTrue(serverListOut[2].hostname.equals("4"));
		assertTrue(serverListOut[3].hostname.equals("2"));
		assertTrue(serverListOut[4].hostname.equals("0"));
		
		ServerRecord target = null;
		
//		Integer key = 0;
//		byte[] hash = null;
//		do {
//			key = key + 1;
//			MessageDigest md = MessageDigest.getInstance("MD5");
//			hash = md.digest(Integer.toString(key).getBytes());
//		} while (hash[0] >= serverListOut[0].hash[0]);
//		System.out.println("Key: " + key);
//		System.out.println("hash[0]: " + hash[0]);
		
		Integer key = 964;
		target = hasher.mapKey(Integer.toString(key));
//		System.out.println(target.hostname);
	
		assertTrue(target.hostname.equals("1"));
	}
	
	@Test
	public void testRemoveServer() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] serverListOut = hasher.getServerList();
//		for (ServerRecord rec : serverListOut) {
//			System.out.print(rec.hostname + ":");
//			for (int i = 0; i < rec.hash.length; i++) {
//				System.out.print(rec.hash[i] + ",");
//			}
//			System.out.println("");
//		}
		assertTrue(serverListOut.length == 5);
		assertTrue(serverListOut[0].hostname.equals("1"));
		assertTrue(serverListOut[1].hostname.equals("3"));
		assertTrue(serverListOut[2].hostname.equals("4"));
		assertTrue(serverListOut[3].hostname.equals("2"));
		assertTrue(serverListOut[4].hostname.equals("0"));
		
		ServerRecord rxServer = new ServerRecord(null);	
		hasher.preRemoveServer(new ServerRecord("2", 0), rxServer);
		assertTrue(rxServer.hostname != null);
		assertTrue(rxServer.hostname.equals("0"));
		
		rxServer = new ServerRecord(null);
		hasher.preRemoveServer(new ServerRecord("0", 0), rxServer);
		assertTrue(rxServer.hostname != null);
		assertTrue(rxServer.hostname.equals("1"));
	}
	
	@Test
	public void testAddServer() throws Exception {
		List<ServerRecord> serverList = new ArrayList<ServerRecord>();
//		serverList.add(new ServerRecord("0", 0));
		serverList.add(new ServerRecord("1", 0));
		serverList.add(new ServerRecord("2", 0));
		serverList.add(new ServerRecord("3", 0));
//		serverList.add(new ServerRecord("4", 0));
		// order: 1, 3, 4, 2, 0
		
		ConsistentHasher hasher = new ConsistentHasher();
		hasher.fromServerList(serverList);
		
		ServerRecord[] serverListOut = hasher.getServerList();
//		for (ServerRecord rec : serverListOut) {
//			System.out.print(rec.hostname + ":");
//			for (int i = 0; i < rec.hash.length; i++) {
//				System.out.print(rec.hash[i] + ",");
//			}
//			System.out.println("");
//		}
		assertTrue(serverListOut.length == 3);
		assertTrue(serverListOut[0].hostname.equals("1"));
		assertTrue(serverListOut[1].hostname.equals("3"));
		assertTrue(serverListOut[2].hostname.equals("2"));
		
		MessageDigest md = MessageDigest.getInstance("MD5");
		
		byte[] primHash3 = md.digest("3:0".getBytes());
		byte[] primHash4 = md.digest("4:0".getBytes());
		byte[] primHash2 = md.digest("2:0".getBytes());
		byte[] primHash0 = md.digest("0:0".getBytes());
		Byte[] hash3 = new Byte[primHash3.length];
		Byte[] hash4 = new Byte[primHash4.length];
		Byte[] hash2 = new Byte[primHash2.length];
		Byte[] hash0 = new Byte[primHash0.length];
		for (int i = 0; i < primHash3.length; i++) {
			hash3[i] = primHash3[i];
		}
		for (int i = 0; i < primHash4.length; i++) {
			hash4[i] = primHash4[i];
		}
		for (int i = 0; i < primHash2.length; i++) {
			hash2[i] = primHash2[i];
		}
		for (int i = 0; i < primHash0.length; i++) {
			hash0[i] = primHash0[i];
		}
		
		ServerRecord txServer = new ServerRecord(null);
		List<Byte> l = new ArrayList<Byte>();
		List<Byte> r = new ArrayList<Byte>();
		hasher.preAddServer(new ServerRecord("4", 0), txServer, l, r);
		assertTrue(txServer.hostname != null);
		assertTrue(txServer.hostname.equals("2"));
		assertTrue(Arrays.equals(l.toArray(), hash3));
		assertTrue(Arrays.equals(r.toArray(), hash4));
		
		txServer = new ServerRecord(null);
		l = new ArrayList<Byte>();
		r = new ArrayList<Byte>();
		hasher.preAddServer(new ServerRecord("0", 0), txServer, l, r);
		assertTrue(txServer != null);
		assertTrue(txServer.hostname.equals("1"));
		assertTrue(Arrays.equals(l.toArray(), hash2));
		assertTrue(Arrays.equals(r.toArray(), hash0));
	}
}
