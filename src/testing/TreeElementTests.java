package testing;

import org.junit.Test;

import common.messages.IVectorClock;
import common.messages.Tree;
import common.messages.TreeElement;
import common.messages.VectorClock;
import junit.framework.TestCase;

public class TreeElementTests extends TestCase {
	@Test
	public void testToString() throws Exception {
		VectorClock c = new VectorClock();
		c.increment("p0");
		c.increment("p1");
		c.increment("p1");
		TreeElement te = new TreeElement(c, "v");
		
		String s = te.toString();
		
		assertTrue(s.equals("p0,1,p1,2,v"));
	}
	
	@Test
	public void testFromString() throws Exception {
		String s = "proc1,5,proc2,7,myvalue";
		
		TreeElement te = TreeElement.fromString(s);
		
		assertTrue(te.value.equals("myvalue"));
		IVectorClock c = te.clock;
		assertTrue(c.at("proc1") == 5);
		assertTrue(c.at("proc2") == 7);
		assertTrue(c.processes().size() == 2);
	}
}