package it.unipr.aotlab.mapreduce.context;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapContext implements Context {

	private static int MAX_FILE_SIZE = 50;
	private List<Entry> bufferedList;
	private long actualSize;
	private int fileIdx = 0;

	public MapContext() {
		bufferedList  = new ArrayList<>();
		fileIdx = 0;
	}

	@Override
	public synchronized void put(Object key, Object value) {
		Entry newEntry = new MyEntry(key, value);
		actualSize += key.toString().length() + value.toString().length() + 1;
		bufferedList.add(newEntry);
		if (actualSize >= MAX_FILE_SIZE) {
			writeInFile();
			actualSize = 0;
		}
	}

	private synchronized void writeInFile() {
		try {
			// create new Context from mapContext where values are grouped by key
			Map<Object, List> mappa = new TreeMap<>();
			for (Entry entry : this.bufferedList) {
				if (mappa.containsKey(entry.getKey())) {
					mappa.get(entry.getKey()).add(entry.getValue());
				} else {
					List tmpList = new ArrayList<>();
					tmpList.add(entry.getValue());
					mappa.put(entry.getKey(), tmpList);
				}
			}

			File file = new File("output/tmp/tmpOut" + fileIdx++ + ".txt");
			file.getParentFile().mkdirs();
			if (!file.exists())
				file.createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));

			for (Entry<Object, List> entry : mappa.entrySet()) {
				bw.write(entry.getKey() + " ");
				List values = entry.getValue();
				for (Object object : values) {
					bw.write(object + " ");
				}
				bw.write("\n");
			}
			bw.close();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void putAll(Map map) {
		Set set = map.entrySet();
		for (Object object : set) {
			Entry entry = ((Entry)object);
			put(entry.getKey(), entry.getValue()); 
		}
	}

	@Override
	public String toString() {
		return String.format("{ dim:%d, %s}", actualSize, bufferedList.toString());
	}

	public List<Entry> getList() {
		return bufferedList;
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

	@Override
	public void close() throws Exception {
	}

}
