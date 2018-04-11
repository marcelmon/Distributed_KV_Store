package testing;

import org.junit.Test;

import common.messages.IVectorClock;
import common.messages.Tree;
import common.messages.TreeElement;
import common.messages.VectorClock;
import junit.framework.TestCase;

public class TreeTests extends TestCase {
	@Test
	public void testPrune() throws Exception {
		Tree a = new Tree("v0", "p0");
		Tree b = new Tree("v1", "p1");
		
		b.merge(a);
		assertTrue(b.getTree().size() == 2);
		b.prune();
		assertTrue(b.getTree().size() == 2);
		
		IVectorClock n = b.next("p0");
		b.getTree().add(new TreeElement(n, "v2"));
		assertTrue(b.getTree().size() == 3);
		
		// the newly added element has knowledge of the other two so pruning should leave only it
		b.prune(); 
		
		assertTrue(b.getTree().size() == 1);
		assertTrue(b.getTree().iterator().next().value.equals("v2"));
	}
	
	@Test
	public void testCollapse() throws Exception {
		Tree tr = new Tree();
		VectorClock c0 = new VectorClock();
		c0.increment("p0");
		VectorClock c1 = new VectorClock();
		c1.increment("p1");
		tr.getTree().add(new TreeElement(c0, "v0"));
		tr.getTree().add(new TreeElement(c1, "v1"));
		tr.collapse("v2", "p0");
		
		assertTrue(tr.getTree().size() == 1);
		IVectorClock c = tr.getTree().iterator().next().clock; 
		assertTrue(c.at("p0") == 2);
		assertTrue(c.at("p1") == 1);
		assertTrue(tr.getTree().iterator().next().value.equals("v2"));
	}
	
	@Test
	public void testToString() throws Exception {
		Tree tr = new Tree();
		VectorClock c0 = new VectorClock();
		c0.increment("p0");
		VectorClock c1 = new VectorClock();
		c1.increment("p1");
		c1.increment("p1");
		tr.getTree().add(new TreeElement(c0, "v0"));
		tr.getTree().add(new TreeElement(c1, "v1"));
		
		String s = tr.toString();
		
		assertTrue(s.equals("p0,1,v0|p1,2,v1"));
	}
	
	//TODO implement additional tests
}
