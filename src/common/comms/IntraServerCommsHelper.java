package common.comms;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class IntraServerCommsHelper {
	public static boolean ResetZookeeper(String zkAddr) throws Exception {
		ZooKeeper zk = new ZooKeeper(zkAddr, 100, null);
		int retryCounter = 0;
		final int maxRetries = 3;
		
		// Clear out cluster:
		List<String> nodes = zk.getChildren(IntraServerComms.clusterGroup, false);
		for (String n : nodes ) {
			try {
				zk.delete(IntraServerComms.clusterGroup + "/" + n, -1);
			} catch (KeeperException e) {
				// do nothing
			}
		}		
		retryCounter = 0;
		while (zk.getChildren(IntraServerComms.clusterGroup, false).size() != 0) {
			retryCounter++;
			if (retryCounter >= maxRetries) return false;
			Thread.sleep(200);
		}
		
		// Clear out rpc:
		List<String> rpcqueue = zk.getChildren(IntraServerComms.rpcGroup, false);
		for (String rpc : rpcqueue ) {
			try {
				zk.delete(IntraServerComms.rpcGroup + "/" + rpc, -1);
			} catch (KeeperException e) {
				// do nothing
			}
		}	
		retryCounter = 0;
		while (zk.getChildren(IntraServerComms.rpcGroup, false).size() != 0) {
			retryCounter++;
			if (retryCounter >= maxRetries) return false;
			Thread.sleep(200);			
		}
			
		return true;
	}
}
