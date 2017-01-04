package it.unipr.aotlab.mapreduce.resources;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapFileContext;
import it.unipr.aotlab.mapreduce.context.MapContext;

public class ResourcesHandler {

	private String inputPath;
	private String outputPath;
	private String tempPath;
	private int blockSize;

	private List<File> inputFiles;
	private List<File> sortedFiles;
	private final Context mapOutputContext;
	private final Context reduceContext;
	
	public ResourcesHandler(String inputPath, String outputPath, int blockSize) {
		super();
		this.inputPath = inputPath;
		this.tempPath = inputPath + "/aaa"; //TODO fix 
		this.outputPath = outputPath;
		this.blockSize = blockSize;
		this.inputFiles = loadPaths(this.inputPath);
		this.sortedFiles = null;
		this.mapOutputContext = new MapContext();// new MapFileContext(tempPath);
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void sortAndGroup() {
		try {
			// create new Context from mapContext where values are grouped by
			// key
			MapContext mContext = (MapContext) mapOutputContext;
			List<Entry> lista = mContext.getList();
			Map<Object, List> mappa = new TreeMap<>();
			for (Entry entry : lista) {
				if (mappa.containsKey(entry.getKey())) {
					mappa.get(entry.getKey()).add(entry.getValue());
				} else {
					List tmpList = new ArrayList<>();
					tmpList.add(entry.getValue());
					mappa.put(entry.getKey(), tmpList);
				}
			}

			File file = new File("resources/tmp/tmpOut.txt");
			file.getParentFile().mkdirs();
			if (!file.exists())
				file.createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));

			for (Entry<Object, List> entry : mappa.entrySet()) {
				bw.write(entry.getKey() + " ");
				List values = entry.getValue();
				for (Object object : values) {
					bw.write(object + " ");
				}
				bw.write("\n");
			}
			bw.close();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		sortedFiles = loadPaths("resources/tmp/");
		
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

}
