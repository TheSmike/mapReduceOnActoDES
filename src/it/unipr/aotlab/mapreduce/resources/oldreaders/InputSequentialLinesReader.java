package it.unipr.aotlab.mapreduce.resources.oldreaders;

import java.io.File;
import java.io.IOException;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class InputSequentialLinesReader extends SequentiallyLinesReader {

	/**
	 * Read a file using {@code SequentiallyLinesReader}
	 * @param file
	 * @param blockSize
	 * 
	 * @see SequentiallyLinesReader
	 */
	public InputSequentialLinesReader(File file, int blockSize) {
		super(file, blockSize);
	}

	/**
	 * Read each lines and return it like String
	 * @return
	 */
	public String readLine() {
		try {
			return super.reader.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}