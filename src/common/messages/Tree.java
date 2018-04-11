package common.messages;

import java.util.Set;

public class Tree implements ITree {
	protected Set<TreeElement> tree;
	
	public Tree(String value, String pName) {
		collapse(value, pName);
	}

	@Override
	public void prune() {
		for (TreeElement x : tree) {
			for (TreeElement y : tree) {
				if (x.clock.happened(y.clock) == IVectorClock.HAPPENED_BEFORE) {
					tree.remove(x);
				}
			}
		}

	}

	@Override
	public void collapse(String value, String pName) {
		TreeElement next = new TreeElement();
		next.clock = next(pName);
		next.value = value;
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

}
