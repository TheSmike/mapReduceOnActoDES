package it.unipr.aotlab.mapreduce.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapContext implements Context {

	public MapContext() {
	}
	
	List<Entry> list = new ArrayList<>();

	@Override
	public synchronized void put(Object key, Object value) {
		list.add(new MyEntry(key, value));
	}

	@Override
	public void putAll(Map map) {
		list.addAll(map.entrySet());
	}

	@Override
	public String toString() {
		return list.toString();
	}
	
	

	private class MyEntry implements Entry {
		Object key;
		Object value;

		public MyEntry(Object key, Object value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public Object getKey() {
			return this.key;
		}

		@Override
		public Object getValue() {
			return this.value;
		}

		@Override
		public Object setValue(Object value) {
			return this.value = value;
		}
		
		@Override
		public String toString() {
			return key + "-" + value;
		}

	}

}
