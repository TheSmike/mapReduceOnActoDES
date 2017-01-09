package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TotalBlockReader {

	private int mapBlocks;
	private Map<Integer, WrapperBlocksReader> keys;
	private Map<String, BlockReader> blockReaders;

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
	
	public int getMapBlocks() {
		return mapBlocks;
	}
	
	public InputLinesReader getInputLinesReader(int index){
		WrapperBlocksReader key = keys.get(index);
		try {
			System.out.println("index " + index + ": " + key.fileName + "-" + key.blockNumber);
			return blockReaders.get(key.fileName).getInputLinesReader(key.blockNumber);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public SortedLinesReader getSortedLinesReader(int index){
		WrapperBlocksReader key = keys.get(index);
		try {
			System.out.println("index " + index + ": " + key.fileName + "-" + key.blockNumber);
			return blockReaders.get(key.fileName).getSortedLinesReader(key.blockNumber);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private int saveKeys(String path, int counter, Map<Integer, WrapperBlocksReader> wrappers, BlockReader br) {
		for (int i = 0; i < br.getTotalBlockNumber(); i++) {
			wrappers.put(counter++, new WrapperBlocksReader(path, i));
		}
		return counter;
	}

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
