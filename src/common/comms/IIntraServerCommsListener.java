package common.comms;

public interface IIntraServerCommsListener {
	/**
     * Called when the hash ring is updated.
     */
    public void consistentHasherUpdated(IConsistentHasher hasher);
    
    /**
     * ECS-related start, starts serving requests
     */
    public void start();

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
     * ECS-related moveData, move the given hashRange to the server going by the targetName
     */
    public boolean moveData(String[] hashRange, String targetName) throws Exception;
}
