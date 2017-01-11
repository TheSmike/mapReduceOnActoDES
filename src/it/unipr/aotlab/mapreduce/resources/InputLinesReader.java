package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;

/**
 * 
 * The class {@code InputLinesReader} define an object that read line by line a block of file at the
 * {@code startPosition} and end when {@code blockSize} is reached. {@code file} is  
 * input file where this operation is applied
 *
 */
public class InputLinesReader extends LinesReader {
	public InputLinesReader(int startPosition, int blockSize, File file) {
		super(startPosition, blockSize, file);
	}

	/**
	 * @return a method read();
	 * 
	 * @see LinesReader
	 */
	public String readLine(){
		try {
			return read();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
