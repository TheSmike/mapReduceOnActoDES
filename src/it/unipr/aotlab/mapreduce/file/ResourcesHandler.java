package it.unipr.aotlab.mapreduce.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.DefaultMapContext;

public class ResourcesHandler {

	private String inputPath;
	private String outputPath;
	private int blockSize;
	private List<File> inputFiles;
	private final Context mapContext;
	private Context sortedContext;

	public ResourcesHandler(String inputPath, String outputPath, int blockSize) {
		super();
		this.inputPath = inputPath;
		inputFiles = loadPaths();
		this.outputPath = outputPath;
		this.blockSize = blockSize;
		this.mapContext = new DefaultMapContext();
		this.sortedContext = null;
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

	public MapperReader getLinesReader(int mapBlock) {
		return new FileMapperReader(inputFiles.get(mapBlock), blockSize);
		// RandomAccessFile raf = null;
		// try {
		// raf = new RandomAccessFile(new File(inputPath), "r");
		// raf.seek(mapBlock * blockSize);
		// byte[] b = new byte[blockSize];
		// raf.read();
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

	}

	public ReducerReader getMappedLinesReader(){
		return new FileReducerReader();
	}
	
	public Context getMapContext() {
		return mapContext;
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
			else{
				File[] files = f.listFiles();
				for (int i = 0; i < files.length; i++) {
					retValue.add(files[i]);
				}
			}
		}
		return retValue;
	}

}
