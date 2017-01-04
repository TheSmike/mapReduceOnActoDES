package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.DefaultContext;
import it.unipr.aotlab.mapreduce.context.FileContext;

public class ResourcesHandler {

//	private String inputPath;
	private String outputPath;
	private int blockSize;
	
	private InputContext inputContext;
	private List<File> inputFiles;
	private final MapContext mapContext;
	private final Context reduceContext;

	public ResourcesHandler(String inputPath, String outputPath, int blockSize) {
		super();
//		this.inputPath = inputPath;
		inputFiles = loadPaths();
		this.outputPath = outputPath;
		this.blockSize = blockSize;
		this.mapContext = new DefaultContext();
		this.reduceContext = new FileContext(outputPath);
	}

	/**
	 * Count number of blocks in file/directory
	 * 
	 * @return
	 */
	public int countMapBlocks() {
		return inputFiles.size();
	}

	public int countReduceBlocks() {
		// TODO Auto-generated method stub
		return 7;
	}

	public InputContext getInputContext(int mapBlock) {
		return new InputContext(inputFiles.get(mapBlock), blockSize);
	}
	
	public MapContext getMapContext() {
		return mapContext;
	}

	public ReducerReader getReducerReader() {
		return new FileReducerReader();
//		return FileMapperReader.getReducerReader();
	}

	public void sortAndGroup() {
		//create new Context from mapContext where values are grouped by key
		//temp 
		//this.mapContext
	}
	


	public Context getReduceContext() {
		return reduceContext;
	}

	// public String getBlock(int mapBlock) {
	// RandomAccessFile raf = null;
	// try {
	// raf = new RandomAccessFile(new File(inputPath), "r");
	// raf.seek(mapBlock * blockSize);
	// byte[] b = new byte[blockSize];
	// raf.readLine();
	// } catch (Exception e) {
	// throw new RuntimeException(e);
	// }finally {
	// if (raf != null)
	// try {
	// raf.close();
	// } catch (IOException e) {
	// throw new RuntimeException(e);
	// }
	// }

	// }

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
