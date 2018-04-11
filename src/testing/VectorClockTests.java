package testing;

import org.junit.Test;

import common.messages.IVectorClock;
import common.messages.VectorClock;
import junit.framework.TestCase;

public class VectorClockTests extends TestCase {
	@Test
	public void testIncrement() throws Exception {
		VectorClock clk = new VectorClock();
		clk.increment("p0");
		
		assertTrue(clk.at("p0") == 1);
		assertTrue(clk.at("p1") == 0);
	}
	
	@Test
	public void testHappened() throws Exception {
		VectorClock c0 = new VectorClock();
		c0.increment("p0");
		c0.increment("p0");
		
		VectorClock c1 = new VectorClock();
		c1.increment("p1");
		c1.increment("p1");
		
		VectorClock c2 = new VectorClock();
		c2.increment("p0");
		c2.increment("p0");
		c2.increment("p0");
		c2.increment("p1");
		c2.increment("p1");
		
		assertTrue(c0.happened(c1) == IVectorClock.HAPPENED_CONCURRENT);
		assertTrue(c2.happened(c0) == IVectorClock.HAPPENED_AFTER);
		assertTrue(c0.happened(c2) == IVectorClock.HAPPENED_BEFORE);
	}
	
	@Test
	public void testMax() throws Exception {
		VectorClock c0 = new VectorClock();
		c0.increment("p1");
		c0.increment("p1");
		
		VectorClock c1 = new VectorClock();
		c1.increment("p0");
		c1.increment("p0");
		c1.increment("p0");
		
		VectorClock c2 = new VectorClock();
		c2.increment("p1");
		
		VectorClock c3 = new VectorClock();
		c3.max(c0);
		assertTrue(c3.at("p0") == 0);
		assertTrue(c3.at("p1") == 2);
		c3.max(c1);
		assertTrue(c3.at("p0") == 3);
		assertTrue(c3.at("p1") == 2);
		c3.max(c2);
		assertTrue(c3.at("p0") == 3);
		assertTrue(c3.at("p1") == 2);
	}
}
