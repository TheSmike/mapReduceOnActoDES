package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;

public class InputLinesReader extends LinesReader {
	public InputLinesReader(int startPosition, int blockSize, File file) {
		super(startPosition, blockSize, file);
	}

	public String readLine(){
		try {
			return read();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
