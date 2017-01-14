package it.unipr.aotlab.mapreduce.context;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class MapFileContext implements Context {

	String outputPath;
	BufferedWriter bw;

	/**
	 * File Context that implements {@link Context}, save data in a single file.
	 * @param outputPath path of output file or folder 
	 */
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
	public void putMap(Map map) {
		Set<Entry> setEntry = map.entrySet();
		for (Entry entry : setEntry) {
			put(entry.getKey(), entry.getValue());
		}

	}

}
