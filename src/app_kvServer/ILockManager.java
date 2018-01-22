package app_kvServer;

public interface ILockManager {

    public enum ResourceType {
        None,
        KEY,
        FILESYSTEM
    };

    /**
     * Get a lock for a resource
     * resources are : KEY, FILESYSTEM
     * If a clockValue is given, check that this lock can be aquired based on this value.
     * The implementation should expect either entries of logical, time based, or vector locks.
     * The case of vector clocks needs to be better researched to determine the expected type. 
     *
     * Consider making a ClockValue object to pass.********
     *
     * @return an integer representing the aquired lock, -1 if fail
     */
    public int getLock(String resource, int resourceType, ClockValue clockValue);

    /**
     * Release a previously aquired lock
     * @return true if lock released, false on error
     */
    public boolean releaseLock(int lockId);

    /**
     * release all locks
     * @return true if all locks released, false on error
     */
    public boolean flushLocks();

}
