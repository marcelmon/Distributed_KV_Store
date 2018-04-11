package common.messages;

public interface ITree {
	/**
	 * Removes all entries from the tree which happened before another entry in the tree.
	 */
	public void prune();
	
	/**
	 * Deletes all entries in the tree and inserts a new entry which happened after all entries.
	 * This method is used when an external mechanism (e.g. user input) determines that the tree
	 * can be reconciled to the single value indicated.
	 */
	public void collapse(String value, String pName);
	
	/**
	 * Combines all nodes in the ITree other with the nodes in this, then prunes the result.
	 */
	public void merge(ITree other);
	
	/**
	 * Returns an IVectorClock instance which represents an event which has knowledge of the tree
	 * and happened at process pName.
	 */
	public IVectorClock next(String pName);
	
	/**
	 * Produces a string representation of the tree to be presented to a user.
	 */
	public String toString();
	
	/**
	 * Returns true if there is only a single entry and thus we can consider the value to be
	 * unambiguous.
	 */
	public boolean unambiguous();
}
