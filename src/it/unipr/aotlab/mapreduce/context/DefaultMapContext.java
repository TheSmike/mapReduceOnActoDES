package it.unipr.aotlab.mapreduce.context;

import java.util.Map;
import java.util.TreeMap;

public class DefaultMapContext implements MapOutputContext {

	Map map = new TreeMap<>();
	
	@Override
	public synchronized void put(Object key, Object value) {
		map.put(key, value);
	}

}
