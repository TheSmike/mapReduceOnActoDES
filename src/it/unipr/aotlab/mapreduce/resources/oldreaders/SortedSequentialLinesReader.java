package it.unipr.aotlab.mapreduce.resources.oldreaders;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import it.unipr.aotlab.mapreduce.resources.SortedLine;
import it.unipr.aotlab.mapreduce.utils.StrUtils;

public class SortedSequentialLinesReader extends SequentiallyLinesReader {

	/**
	 * Read a file using {@code SequentiallyLinesReader}
	 * 
	 * @param file
	 * @param blockSize
	 */
	public SortedSequentialLinesReader(File file, int blockSize) {
		super(file, blockSize);
	}

	/**
	 * Read each lines and return it like {@link SortedLine}
	 * 
	 * @return
	 */
	public SortedLine readLine() {
		try {
			String line = super.reader.readLine();
			if (StrUtils.isEmpty(line))
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