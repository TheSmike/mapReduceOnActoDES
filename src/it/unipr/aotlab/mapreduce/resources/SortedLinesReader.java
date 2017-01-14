package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import it.unipr.aotlab.mapreduce.utils.StrUtils;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class SortedLinesReader extends LinesReader {

	/**
	 * The class {@code SortedLinesReader} define an object that read line by line a block of file at the
	 * {@code startPosition} and end when {@code blockSize} is reached. {@code file} is  
	 * input file where this operation is applied	
	 * @param startPosition
	 * @param blockSize
	 * @param file
	 */
	public SortedLinesReader(int startPosition, int blockSize, File file) {
		super(startPosition, blockSize, file);
	}


	/**
	 * Read a line 
	 * @return a line in {@link SortedLine} format
	 */
	public SortedLine readLine() {
		try {
			String line = super.read();
			if(StrUtils.isEmpty(line))
				return null;
			
			String[] array = line.split(" ", 2);
			if (array.length >= 2)
				return new SortedLine(array[0], Arrays.asList(array[1].split(" ")));
			else
				return new SortedLine(array[0], new ArrayList<>());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
