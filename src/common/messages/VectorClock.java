package common.messages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VectorClock implements IVectorClock {
	protected Map<String, Integer> map;	
	
	public VectorClock() {
		map = new HashMap<String, Integer>();
	}
	
	@Override
	public int at(String pName) {
		if (map.containsKey(pName)) {
			return map.get(pName);
		} else {
			return 0;
		}
	}

	@Override
	public void increment(String pName) {
		if (map.containsKey(pName)) {
			map.put(pName, map.get(pName)+1);
		} else {
			map.put(pName, 1);
		}
	}

	@Override
	public int happened(IVectorClock c) {
		// ALGORITHM NOTES:
		// Happened before if a) no element is greater, and b) there exists at least one 
		// element which is less
		//
		// Happened after if a) no element is less, and b) there exists at least one element
		// which is greater.
		//
		// Concurrent otherwise.
		
		boolean existsGreater = false;
		boolean existsLess = false;
		
		Set<String> processes = this.unionProcesses(c);
		
		for (String p : processes) {
			int x = this.at(p);
			int y = c.at(p);
			if (x < y) existsLess = true;
			else if (x > y) existsGreater = true;
			if (existsLess && existsGreater) break;
		}
		
		if (existsLess && !existsGreater) return HAPPENED_BEFORE;
		else if (!existsLess && existsGreater) return HAPPENED_AFTER;
		else return HAPPENED_CONCURRENT;
	}

	@Override
	public Set<String> unionProcesses(IVectorClock c) {
		HashSet<String> processesThis = new HashSet<String>(map.keySet());
		HashSet<String> processesThat = new HashSet<String>(c.processes());
		processesThis.addAll(processesThat);
		return processesThis;
	}

	@Override
	public Set<String> processes() {
		return map.keySet();
	}

	@Override
	public void max(IVectorClock c) {
		Set<String> processes = this.unionProcesses(c);
		for (String p : processes) {
			map.put(p, Math.max(this.at(p), c.at(p)));
		}
	}
	
	@Override
	public String toString() {
		String output = "";
		for (String k : map.keySet()) {
			output += k + "," + map.get(k) + ",";
		}
		return output.substring(0, output.length()-1); // remove trailing comma
	}
	
	@Override
	public void fromString(String s) {
		String[] spl = s.split(",");
		if (spl.length == 0) {
			System.out.println("ERROR! Invalid formatting of IVectorClock string. Length = 0");
			return;
		}
		
		if (spl.length % 2 != 0) {
			System.out.println("ERROR! Invalid formatting of IVectorClock string. Uneven number of elements.");
			return;
		}
		
		map.clear();
		try {
			for (int i = 0; i < spl.length; i+=2) {
				map.put(spl[i], Integer.parseInt(spl[i+1]));
			}
		} catch (NumberFormatException e) {
			System.out.println("ERROR! Invalid formatting of IVectorClock string. Count not integer");
			return;
		}
	}
	
	@Override
	public boolean equals(IVectorClock c) {
		Set<String> proc = unionProcesses(c);
		for (String p : proc) {
			if (at(p) != c.at(p)) return false;
		}
		return true;
	}

}
