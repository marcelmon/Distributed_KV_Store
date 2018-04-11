package common.messages;

public interface IVectorClock {
	/**
	 * Returns the clock value for process pName.
	 */
	public int at(String pName);
	
	/**
	 * Increments the clock value for process pName.
	 */
	public void increment(String pName);
	
	/**
	 * Compares this to the IVectorClock c.
	 * @return >0 if this happened after c, <0 if this happened before c, or 0 if concurrent.
	 */
	public int happened(IVectorClock c);
}
