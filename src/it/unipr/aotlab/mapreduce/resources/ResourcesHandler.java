package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapFileContext;
import it.unipr.aotlab.mapreduce.context.MapContext;

public class ResourcesHandler {

	private String inputPath;
	private String outputPath;
	private int blockSize;

	private List<File> inputFiles;
	private List<File> sortedFiles;
	private final Context mapOutputContext;
	private final Context reduceContext;
	private String tempPath;

	public ResourcesHandler(String inputPath, String outputPath, int blockSize) {
		super();
		this.inputPath = inputPath;
		this.tempPath = inputPath + "/aaa";
		this.outputPath = outputPath;
		this.blockSize = blockSize;
		this.inputFiles = loadPaths();
		this.sortedFiles = null;
		this.mapOutputContext = new MapContext();//new MapFileContext(tempPath);
		this.reduceContext = new MapFileContext(outputPath);
	}

	/**
	 * Count number of blocks in file/directory
	 * 
	 * @return
	 */
	public int countMapBlocks() {
		return inputFiles.size();
	}

	public InputLinesReader getInputLinesReader(int blockNumber) {
		return new InputLinesReader(inputFiles.get(blockNumber), blockSize);
	}

	public Context getMapContext() {
		return mapOutputContext;
	}

	public void sortAndGroup() {
		// create new Context from mapContext where values are grouped by key
		// temp
		// this.mapContext
		this.sortedFiles = new ArrayList<>();
	}

	public int countReduceBlocks() {
		// TODO Auto-generated method stub
		return 7;
		//return this.sortedFiles.size();
	}
	public SortedLinesReader getSortedLinesReader(int blockNumber) {
		return new SortedLinesReader(sortedFiles.get(blockNumber), this.blockSize);
	}

	public Context getReduceContext() {
		return reduceContext;
	}

	private List<File> loadPaths() {
		List<File> retValue = new ArrayList<>();
		File f = new File(this.inputPath);
		if (f.exists()) {
			if (f.isFile())
				retValue.add(f);
			else {
				File[] files = f.listFiles();
				for (int i = 0; i < files.length; i++) {
					retValue.add(files[i]);
				}
			}
		}
		return retValue;
	}

}
