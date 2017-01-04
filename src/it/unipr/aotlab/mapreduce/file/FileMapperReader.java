package it.unipr.aotlab.mapreduce.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileMapperReader implements MapperReader {

	// private int blockSize;
	private File fileToRead;
	private BufferedReader reader;

	public FileMapperReader(File file, int blockSize) {
		this.fileToRead = file;
		reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileToRead));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		// this.blockSize = blockSize;
	}

	/* (non-Javadoc)
	 * @see it.unipr.aotlab.mapreduce.file.ILinesReader#readLine()
	 */
	@Override
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
