package it.unipr.aotlab.mapreduce.context;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MapFileContext implements Context, AutoCloseable {

	String outputPath;
	BufferedWriter bw;

	public MapFileContext(String outputPath) {
		try {
			this.outputPath = outputPath;
			File f = new File(outputPath);
			if (f.exists() && f.isFile())
				throw new RuntimeException("outputPath is a File, expected directory");
			f.mkdirs();
			f = new File(f, "output1.txt");
			bw = new BufferedWriter(new FileWriter(f));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void put(Object key, Object value) {
		try {
			bw.write(key + " " + value + "\n");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws Exception {
		if (bw != null) {
			bw.flush();
			bw.close();
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void putAll(Map map) {
		Set<Entry> setEntry = map.entrySet();
		for (Entry entry : setEntry) {
			put(entry.getKey(), entry.getValue());
		}

	}

}
