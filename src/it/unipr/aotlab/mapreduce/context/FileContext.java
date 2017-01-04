package it.unipr.aotlab.mapreduce.context;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class FileContext implements Context, AutoCloseable {

	String outputPath;
	BufferedWriter bw;

	public FileContext(String outputPath) {
		try {
			this.outputPath = outputPath;
			File f = new File(outputPath);
			if (f.isDirectory()) {
				if (!f.exists())
					f.mkdir();
				f = new File(f, "output1.txt");
			} else {
				if (!f.exists())
					f.createNewFile();
			}
			bw = new BufferedWriter(new FileWriter(f));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(Object key, Object value) {
		try {
			bw.write(key + " " + value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws Exception {
		if (bw != null)
			bw.close();

	}

}
