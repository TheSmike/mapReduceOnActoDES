package it.unipr.aotlab.mapreduce.context;

import java.util.Map;
import java.util.TreeMap;

public class DefaultMapContext implements Context {

	Map map = new TreeMap<>();
	
	@Override
	public synchronized void put(Object key, Object value) {
		map.put(key, value);
	}
	
	@Override
	public void putAll(Map map) {
		map.putAll(map);
	}
	
	@Override
	public String toString() {
		return map.toString();
	}



}
