package common.messages;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Tree implements ITree {
	protected Set<TreeElement> tree = new HashSet<TreeElement>();
	
	public Tree() { }
	
	public Tree(String value, String pName) {
		collapse(value, pName);
	}

	@Override
	public void prune() {
		ArrayList<TreeElement> removals = new ArrayList<TreeElement>();
		for (TreeElement x : tree) {
			for (TreeElement y : tree) {
				if (x.clock.happened(y.clock) == IVectorClock.HAPPENED_BEFORE) {
					removals.add(x);
				}
			}
		}
		for (TreeElement x : removals) {
			tree.remove(x);
		}
	}

	@Override
	public void collapse(String value, String pName) {
		TreeElement next = new TreeElement(next(pName), value);
		tree.clear();
		tree.add(next);
	}

	@Override
	public void merge(ITree other) {
		// Get the union of the trees
		tree.addAll(other.getTree());
		
		// Prune:
		prune();
	}

	@Override
	public IVectorClock next(String pName) {
		VectorClock output = new VectorClock(); // this will be empty
		for (TreeElement v : tree) {
			output.max(v.clock);
		}
		output.increment(pName);
		return output;
	}

	@Override
	public boolean unambiguous() {
		return tree.size() == 1;
	}

	@Override
	public Set<TreeElement> getTree() {
		return tree;
	}

	@Override
	public boolean empty() {
		return tree.isEmpty();
	}

	@Override
	public String getSingle() {
		if (unambiguous()) {
			return tree.iterator().next().value;
		} else {
			return null;
		}
	}

	@Override
	public String display() {
		return null;
	}

	@Override
	public String toString() {
		String output = "";
		for (TreeElement e : tree) {
			output += e.toString() + ";";
		}
		return output.substring(0, output.length()-1); // remove final comma
	}
	
	@Override
	public void fromString(String s) {
		String[] spl = s.split(";");
		tree.clear();
		for (String elem : spl) {
			tree.add(TreeElement.fromString(elem));
		}
	}

	@Override
	public boolean equals(Tree t) {
		for (TreeElement x : tree) {
			boolean found = false;
			for (TreeElement y : t.getTree()) {
				found |= x.equals(y);
				continue;
			}
			if (!found) return false;
		}
		
		for (TreeElement x : t.getTree()) {
			boolean found = false;
			for (TreeElement y : tree) {
				found |= x.equals(y);
				continue;
			}
			if (!found) return false;
		}
		
		return true;
	}
}
