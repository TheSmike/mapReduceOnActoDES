package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import it.unipr.aotlab.mapreduce.utils.StrUtils;

public class SortedLinesReader extends LinesReader {
	public SortedLinesReader(int startPosition, int blockSize, File file) {
		super(startPosition, blockSize, file);
	}

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
