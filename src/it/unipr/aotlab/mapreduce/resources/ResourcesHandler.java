package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.util.Date;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapContext;
import it.unipr.aotlab.mapreduce.context.MapFileContext;

/**
 * 
 * @author Scarpenti 
 * @author Viti 
 * This class maintains some information about file on FS for use inside the operation of map/reduce.
 *
 */
public class ResourcesHandler {

	private final String tmpPath;
	private String inputPath;
	private int blockSize;

	private TotalBlockReader inputBlockReader;
	private TotalBlockReader sortedBlockReader;
	private final Context mapOutputContext;
	private final Context reduceContext;

	/**
	 * 
	 *  This class maintains some information about file on FS for use inside the operation of map/reduce.
	 *  
	 * 
	 * @param inputPath
	 *            path of the input directory
	 * @param outputPath
	 *            path of the output directory
	 * @param blockSize
	 *            the size of each block in wich the single file is splitted
	 * @param bufferedContextSize 
	 * 			  size of buffered input Context in memory, if size exceeds the buffer is write in file and emptied
	 */
	public ResourcesHandler(String inputPath, String outputPath, int blockSize, int bufferedContextSize) {
		super();
		this.inputPath = inputPath;
		this.tmpPath = outputPath + "tmp" + getIndex() + "/";
		this.blockSize = blockSize;
		this.inputBlockReader = new TotalBlockReader(this.inputPath, blockSize);
		this.sortedBlockReader = null;
		this.mapOutputContext = new MapContext(this.tmpPath, bufferedContextSize);
		this.reduceContext = new MapFileContext(outputPath);
	}

	/**
	 * return number of blocks in input file or directory
	 * 
	 * @return
	 */
	public int countMapBlocks() {
		return this.inputBlockReader.getMapBlocks();
	}

	/**
	 * 
	 * @param blockNumber
	 *            the number of the block that identify a piece of file
	 * @return new instance of {@code InputLinesReader} associated with selected {@code blockNumber}
	 */
	public InputLinesReader getInputLinesReader(int blockNumber) {
		return inputBlockReader.getInputLinesReader(blockNumber);
	}

	/**
	 * @return OutputContext for map operation
	 */
	public Context getMapContext() {
		return mapOutputContext;
	}

	/**
	 * @return return blocks number of sorted file
	 */
	public int countReduceBlocks() {
		return this.sortedBlockReader.getMapBlocks();
	}

	/**
	 * @param blockNumber
	 *            the number of the block that identify a piece of file
	 * @return new instance of {@code SortedLinesReader} associated with selected {@code blockNumber}
	 */
	public SortedLinesReader getSortedLinesReader(int blockNumber) {
		return sortedBlockReader.getSortedLinesReader(blockNumber);
	}

	/**
	 * @return OutputContext for reduce operation
	 */
	public Context getReduceContext() {
		return reduceContext;
	}

	/**
	 * invoke Close method of ReduceContext
	 */
	public void closeReduceContext() {
		try {
			reduceContext.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Delete all temp file generated in MapReduce process (sort files)
	 */
	public void deleteTmpFiles() {
		File f = new File(tmpPath);
		// File f2 = new File(f.getParentFile(), "/tmp"+ (new Date()).getTime()
		// + "/");
		// f2.mkdir();
		for (File file : f.listFiles()) {
			file.delete();
			// file.renameTo(new File(f2, file.getName()));
		}

	}

	/**
	 * Sort all temp file in a single sorted files
	 */
	public void sortAndGroup() {
		((MapContext) mapOutputContext).sortAll();
		sortedBlockReader = new TotalBlockReader(tmpPath + "/sorted", blockSize);
	}

	/**
	 *
	 * @return  an execution unique id
	 */
	private long getIndex() {
		return (new Date().getTime());
	}

}
