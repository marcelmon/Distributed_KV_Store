package common.comms;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class IntraServerComms implements IIntraServerComms, Watcher {
	protected ZooKeeper zk = null;
	protected final String clusterGroup = "/cluster";
	protected String me = null;
	protected IIntraServerCommsListener listener = null;
	
	public IntraServerComms(String zkAddr) throws Exception {		
		zk = new ZooKeeper(zkAddr, 100, this);
		
		// If cluster group doesn't exist, create it
		if (zk.exists(clusterGroup, false) == null) {
			zk.create(clusterGroup, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	@Override
	public void close() throws Exception {
		zk.close();
	}
	
	protected void finalize() throws Exception {
		close();
	}
	
	@Override
	public boolean call(String target, RPCMethod method, String... args) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void register(IIntraServerCommsListener server) {
		listener = server;
	}

	@Override
	public void addServer(String hostname, Integer port) throws ServerExistsException, Exception {
		try {
			zk.create(
					clusterGroup + "/" + hostname + ":" + port, 
					"".getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL);
			me = hostname + ":" + port;
		} catch (NodeExistsException e) {
			throw new ServerExistsException(hostname + ":" + port);
		} catch (InterruptedException e) {
			throw e;
		}
		
	}

	@Override
	public void removeServer() throws NotYetRegisteredException, Exception {
		if (me == null) {
			throw new NotYetRegisteredException("Attempted to call removeServer() on a server never registered in the cluster");
		}
		try {
			zk.delete(clusterGroup + "/" + me, -1); // any version
		} catch (KeeperException e) {
			throw e;
		} catch (InterruptedException e) {
			throw e;
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (listener != null) {
			// TODO listen for rpcs directed at "me"
			// TODO redirect to listener
		}
	}

}
