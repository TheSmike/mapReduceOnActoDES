package it.unipr.aotlab.mapreduce.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public abstract class LinesReader implements AutoCloseable {

	private File fileToRead;
	protected BufferedReader reader;

	public LinesReader(File file, int blockSize) {
		this.fileToRead = file;
		reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileToRead));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	}

	// RandomAccessFile raf = null;
	// try {
	// raf = new RandomAccessFile(new File(inputPath), "r");
	// raf.seek(mapBlock * blockSize);
	// byte[] b = new byte[blockSize];
	// raf.read();
	// } catch (Exception e) {
	// throw new RuntimeException(e);
	// }finally {
	// if (raf != null)
	// try {
	// raf.close();
	// } catch (IOException e) {
	// throw new RuntimeException(e);
	// }
	// }
}
