package common.messages;

import java.util.Set;

public interface IVectorClock {
	static final int HAPPENED_BEFORE = -1;
	static final int HAPPENED_AFTER = 1;
	static final int HAPPENED_CONCURRENT = 0;
	
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
	
	/**
	 * Returns a set containing all the processes in this.
	 */
	public Set<String> processes();
	
	/**
	 * Returns a set containing the union of all the processes in this and all the processes
	 * in c.
	 */
	public Set<String> unionProcesses(IVectorClock c);
	
	/**
	 * Sets all clock values to the max of this and c.
	 */
	public void max(IVectorClock c);
		
	public String toString();
	
	public void fromString(String s);
	
	public boolean equals(IVectorClock c);
}
