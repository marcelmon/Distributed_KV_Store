package common.messages;

import java.util.Arrays;

public class TreeElement {
	public IVectorClock clock;
	public String value;
	public TreeElement(IVectorClock clock, String value) {
		this.clock = clock;
		this.value = value;
	}
	public String toString() {
		return clock.toString() + "," + value;
	}
	public static TreeElement fromString(String s) {
		String[] spl = s.split(",");
		if (spl.length % 2 != 1) {
			System.out.println("ERROR! Invalid string representation of TreeElement. Length not odd.");
			return null;
		}
		
		IVectorClock c = new VectorClock();
		c.fromString(s.substring(0, s.length() - spl[spl.length-1].length() - 1));
		return new TreeElement(c, spl[spl.length - 1]);
	}
	public boolean equals(TreeElement e) {
		return clock.equals(e.clock) && value.equals(e.value);
	}
}
