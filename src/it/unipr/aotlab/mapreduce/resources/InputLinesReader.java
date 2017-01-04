package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;

public class InputLinesReader extends LinesReader {

	public InputLinesReader(File file, int blockSize) {
		super(file, blockSize);
	}

	public String readLine() {
		try {
			return super.reader.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}