package app_kvServer;

import java.time.Duration;

public interface ILockManager {
	/**
	 * Thrown if a lock is released which is not held
	 */
	public class LockNotHeldException extends Exception {
		private static final long serialVersionUID = 1L;

		public LockNotHeldException(String msg) {
			super(msg);
		}
	}
	
	/**
	 * Thrown if a lock is requested but is already held by the caller.
	 */
	public class LockAlreadyHeldException extends Exception {
		private static final long serialVersionUID = 1L;

		public LockAlreadyHeldException(String msg) {
			super(msg);
		}		
	}
	
    /**
     * Get a lock on a key. Blocks until lock is acquired or timeout occurs. Notably, key needn't
     * already exist (as would be the case with a new key).
     *
     * @param timeout The amount of time to wait for the lock to be acquired, after which the 
     * method returns without the lock (returning false). If (timeout.isZero() == true) blocks
     * indefinitely or until the lock is acquired. Behavior undefined for (timeout.isNegative() == true). 
     * @return true if lock is acquired, or false if timed out
     */
    public boolean getLock(String key, Duration timeout) throws LockAlreadyHeldException;

    /**
     * Release a previously acquired lock.
     */
    public void releaseLock(String key) throws LockNotHeldException;

    /**
     * Release all locks.
     */
    public void flushLocks();

}
