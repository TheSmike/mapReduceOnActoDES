package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class SortedLinesReader extends LinesReader {

	public SortedLinesReader(File file, int blockSize) {
		super(file, blockSize);
	}

	public SortedLine readLine() {
		try {
			String line = super.reader.readLine();
			String[] array = line.split(" ", 1);
			if (array.length >= 2)
				return new SortedLine(array[0], Arrays.asList(array[1].split(" ")));
			else
				return new SortedLine(array[0], new ArrayList<>());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}