package common.comms;

public interface IIntraServerCommsListener {
	/**
     * Called when the hash ring is updated.
     */
    public void consistentHasherUpdated(IConsistentHasher hasher);
    
    /**
     * ECS-related start, starts serving requests
     */
    public void start() throws Exception;

    /**
     * ECS-related stop, stops serving requests
     */
    public void stop();

    /**
     * ECS-related lock, locks the KVServer for write operations
     */
    public void lockWrite();

    /**
     * ECS-related unlock, unlocks the KVServer for write operations
     */
    public void unlockWrite();
    
    /**
     * ECS-related. If a client request is made but this server is not responsible,
     * reject that request and send the client the ConsistentHasher.
     */
    public void enableRejectIfNotResponsible();
    
    /**
     * ECS-related. If a client request is made, execute regardless of whether
     * the server is responsible. Used to simulate a network partition.
     */
    public void disableRejectIfNotResponsible();
    
    /**
     * ECS-related. Send all key/value pairs which this server isn't responsible for
     * to the server responsible. Used to repair after a (simulated) network partitions.
     */
    public void sync();

    /**
     * ECS-related moveData, move the given hashRange to the server going by the targetName
     */
    public boolean moveData(String[] hashRange, String targetName) throws Exception;
}
