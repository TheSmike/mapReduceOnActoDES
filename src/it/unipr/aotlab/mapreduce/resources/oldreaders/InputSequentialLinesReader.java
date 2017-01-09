package it.unipr.aotlab.mapreduce.resources.oldreaders;

import java.io.File;
import java.io.IOException;

public class InputSequentialLinesReader extends SequentiallyLinesReader {

	public InputSequentialLinesReader(File file, int blockSize) {
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