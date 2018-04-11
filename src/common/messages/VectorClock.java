package common.messages;

import java.util.HashMap;
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
		Set<String> processesThis = map.keySet();
		Set<String> processesThat = c.processes();
		processesThis.addAll(processesThat);
		return processesThis;
	}

	@Override
	public Set<String> processes() {
		return map.keySet();
	}

}
