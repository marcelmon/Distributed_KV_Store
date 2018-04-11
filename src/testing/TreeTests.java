package testing;

import java.util.Iterator;
import java.util.Set;

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
	public void testNext() throws Exception {
		Tree tr = new Tree();
		VectorClock c0 = new VectorClock();
		c0.increment("p0");
		VectorClock c1 = new VectorClock();
		c1.increment("p1");
		c1.increment("p1");
		tr.getTree().add(new TreeElement(c0, "v0"));
		tr.getTree().add(new TreeElement(c1, "v1"));
		
		IVectorClock c = tr.next("p1");
		assertTrue(c.at("p0") == 1);
		assertTrue(c.at("p1") == 3);
		assertTrue(c.processes().size() == 2);
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
		
		assertTrue(s.equals("p0,1,v0;p1,2,v1") || s.equals("p1,2,v1;p0,1,v0"));
	}
	
	@Test
	public void testFromString() throws Exception {
		String s = "procA,4,procB,5,myvalue;pX,2,another";
		Tree tr = new Tree();
		tr.fromString(s);
		
		Set<TreeElement> elems = tr.getTree();
		Iterator<TreeElement> it = elems.iterator();
		TreeElement e0 = it.next();
		TreeElement e1 = it.next();
		assertFalse(it.hasNext());
		
	    assertTrue(e0.clock.at("procA") == 4);
	    assertTrue(e0.clock.at("procB") == 5);
	    assertTrue(e0.clock.processes().size() == 2);
	    assertTrue(e0.value.equals("myvalue"));
	    assertTrue(e1.clock.at("pX") == 2);
	    assertTrue(e1.clock.processes().size() == 1);
	    assertTrue(e1.value.equals("another"));
	}
	
	@Test
	public void testEquals() throws Exception {
		String s0 = "procA,4,procB,5,myvalue;pX,2,another";
		String s1 = "procA,4,procB,5,myvalue;pX,2,another;pY,5,yetanother";
		
		Tree t0 = new Tree();
		t0.fromString(s0);
		
		Tree t1 = new Tree();
		t1.fromString(s0);
		
		Tree t2 = new Tree();
		t2.fromString(s1);
		
		assertTrue(t0.equals(t1));
		assertFalse(t0.equals(t2));
	}
}
