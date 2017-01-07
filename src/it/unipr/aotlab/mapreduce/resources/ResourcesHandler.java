package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapContext;
import it.unipr.aotlab.mapreduce.context.MapFileContext;

public class ResourcesHandler {

	private final String tmpPath;
	private String inputPath;
	private int blockSize;

	private List<File> inputFiles;
	private List<File> sortedFiles;
	private final Context mapOutputContext;
	private final Context reduceContext;
	
	public ResourcesHandler(String inputPath, String outputPath, int blockSize) {
		super();
		this.inputPath = inputPath;
		this.tmpPath = outputPath + "tmp" + getIndex() + "/";
		this.blockSize = blockSize;
		this.inputFiles = loadPaths(this.inputPath);
		this.sortedFiles = null;
		this.mapOutputContext = new MapContext(this.tmpPath);
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

	/**
	 * 
	 * @param blockNumber
	 * @return
	 */
	public InputLinesReader getInputLinesReader(int blockNumber) {
		return new InputLinesReader(inputFiles.get(blockNumber), blockSize);
	}

	public Context getMapContext() {
		return mapOutputContext;
	}

	public int countReduceBlocks() {
		return this.sortedFiles.size();
	}

	public SortedLinesReader getSortedLinesReader(int blockNumber) {
		return new SortedLinesReader(sortedFiles.get(blockNumber), this.blockSize);
	}

	public Context getReduceContext() {
		return reduceContext;
	}

	private static List<File> loadPaths(String path) {
		List<File> retValue = new ArrayList<>();
		File f = new File(path);
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

	public void closeReduceContext() {
		try {
			reduceContext.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

	public void deleteTmpFiles() {
		File f = new File(tmpPath);
//		File f2 = new File(f.getParentFile(), "/tmp"+ (new Date()).getTime() + "/");
//		f2.mkdir();
		for (File file : f.listFiles()) {
			file.delete();
//			file.renameTo(new File(f2, file.getName()));
		}
		
	}

	public void sortAndGroup() {
		((MapContext)mapOutputContext).sortAll();
		sortedFiles = loadPaths(tmpPath + "/sorted");
	}
	
	private long getIndex() {
		// TODO Auto-generated method stub
		return (new Date().getTime());
	}

}
