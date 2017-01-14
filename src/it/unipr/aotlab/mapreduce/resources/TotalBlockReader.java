package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class TotalBlockReader {

	private int mapBlocks;
	private Map<Integer, WrapperBlocksReader> keys;
	private Map<String, BlockReader> blockReaders;

	/**
	 * Rappresent a blocks collection of all files inside a folder.
	 * Each block has an integer unique key that identify the block in the folder
	 * @param path folder path
	 * @param blockSize approximately block maximum size
	 */
	public TotalBlockReader(String path, int blockSize) {
		try {
			int counter = 0;
			mapBlocks = 0;
			keys = new HashMap<>();
			blockReaders = new HashMap<>();
			
			File f = new File(path);
			if (f.exists()) {
				if (f.isFile()) {
					BlockReader br = new BlockReader(path, blockSize);
					mapBlocks += br.getTotalBlockNumber();
					blockReaders.put(path, br);
					counter = saveKeys(path, counter, keys, br);
				} else {
					if (!path.endsWith("/"))
						path += "/";
					File[] files = f.listFiles();
					for (int i = 0; i < files.length; i++) {
						BlockReader br = new BlockReader(files[i].getAbsolutePath(), blockSize);
						mapBlocks += br.getTotalBlockNumber();
						blockReaders.put(files[i].getAbsolutePath(), br);
						counter = saveKeys(files[i].getAbsolutePath(), counter, keys, br);
					}
				}
			}
			System.out.println(keys);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 
	 * @return blocks number
	 */
	public int getMapBlocks() {
		return mapBlocks;
	}
	
	/**
	 * return reader on selected block
	 * @param index unique key that identify the block in the folder
	 * @return new instance of {@link InputLinesReader} about the block selected with index
	 */
	public InputLinesReader getInputLinesReader(int index){
		WrapperBlocksReader key = keys.get(index);
		try {
			System.out.println("index " + index + ": " + key.fileName + "-" + key.blockNumber);
			return blockReaders.get(key.fileName).getInputLinesReader(key.blockNumber);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * return reader on selected block in {@link SortedLinesReader} format
	 * @param index unique key that identify the block in the folder
	 * @return new instance of {@link SortedLinesReader} about the block selected with index
	 */
	public SortedLinesReader getSortedLinesReader(int index){
		WrapperBlocksReader key = keys.get(index);
		try {
			System.out.println("index " + index + ": " + key.fileName + "-" + key.blockNumber);
			return blockReaders.get(key.fileName).getSortedLinesReader(key.blockNumber);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * couple unique index (key) to specific file and block number, save this information in {@code wrappers}
	 * @param path name of selected file 
	 * @param counter unique index
	 * @param wrappers {@code Map} containing couples
	 * @param br {@code BlockReader} associated to   selected file 
	 * @return the next unique index
	 */
	private int saveKeys(String path, int counter, Map<Integer, WrapperBlocksReader> wrappers, BlockReader br) {
		for (int i = 0; i < br.getTotalBlockNumber(); i++) {
			wrappers.put(counter++, new WrapperBlocksReader(path, i));
		}
		return counter;
	}

	/**
	 * Couple [key, (filename, blocknumber)] 
	 *
	 */
	public class WrapperBlocksReader {
		String fileName;
		int blockNumber;
		public WrapperBlocksReader(String fileName, int blockNumber) {
			this.fileName = fileName;
			this.blockNumber = blockNumber;
		} 
		
		@Override
		public String toString() {
			return "["+ fileName +", " + blockNumber +"]\n";
		}
		
		
	}
	
}
