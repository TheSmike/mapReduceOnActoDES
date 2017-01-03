package it.unipr.aotlab.mapreduce.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class LinesReader implements AutoCloseable {

	// private int blockSize;
	private File fileToRead;
	private BufferedReader reader;

	public LinesReader(File file, int blockSize) {
		this.fileToRead = file;
		reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileToRead));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		// this.blockSize = blockSize;
	}

	public String readLine() {
		try {
			return reader.readLine();
		} catch (IOException e) {
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

	// public boolean hasNext() {
	// }
}
