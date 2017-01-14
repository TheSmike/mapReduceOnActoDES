package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class InputLinesReader extends LinesReader {
	
	/**
	 * The class {@code InputLinesReader} define an object that read line by line a block of file at the
	 * {@code startPosition} and end when {@code blockSize} is reached. {@code file} is  
	 * input file where this operation is applied	
	 * @param startPosition
	 * @param blockSize
	 * @param file
	 */
	public InputLinesReader(int startPosition, int blockSize, File file) {
		super(startPosition, blockSize, file);
	}

	/**
	 * @return value from {@link LinesReader#read()}
	 * 
	 * @see LinesReader
	 */
	public String readLine(){
		try {
			return super.read();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
