package it.unipr.aotlab.mapreduce.context;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * 
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapContext implements Context {

	private static int MAX_FILE_SIZE = 1024 * 10;
	private List<Entry> bufferedList;
	private long actualSize;
	private int fileIdx = 0;
	private final String tmpPath;

	public MapContext(String tmpPath) {
		this.tmpPath = tmpPath;
		bufferedList = new ArrayList<>();
		fileIdx = 0;
	}

	@Override
	public synchronized void put(Object key, Object value) {
		Entry newEntry = new MyEntry(key, value);
		actualSize += key.toString().length() + value.toString().length() + 1;
		bufferedList.add(newEntry);
		if (actualSize >= MAX_FILE_SIZE) {
			writeInFile();
			actualSize = 0;
			bufferedList.clear();
		}
	}

	/**
	 * 
	 * build a structure key - list of values creating a group of key-value pair
	 * and create by this group a key - list of values association. After that
	 * operation write this output on a file
	 * 
	 */
	private synchronized void writeInFile() {
		try {
			System.out.println(this.toString());
			// create new Context from mapContext where values are grouped by
			// key
			TreeMap<String, List> mappa = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
			for (Entry entry : this.bufferedList) {
				if (mappa.containsKey(entry.getKey())) {
					mappa.get(entry.getKey()).add(entry.getValue());
				} else {
					List tmpList = new ArrayList<>();
					tmpList.add(entry.getValue());
					mappa.put((String)entry.getKey(), tmpList);
				}
			}
			System.out.println(mappa);
			File file = new File(tmpPath + "tmp" + fileIdx++ + ".txt");
			file.getParentFile().mkdirs();
			if (!file.exists())
				file.createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));

			for (Entry<String, List> entry : mappa.entrySet()) {
				System.out.println(entry.getKey());
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

	}

	@Override
	public void putAll(Map map) {
		Set set = map.entrySet();
		for (Object object : set) {
			Entry entry = ((Entry) object);
			put(entry.getKey(), entry.getValue());
		}

	}

	@Override
	public String toString() {
		return String.format("{ dim:%d, %s}", actualSize, bufferedList.toString());
	}

	@Override
	public void close() throws Exception {

	}

	/**
	 * 
	 * Make the sort operation with all files generated with map operation and
	 * create a final sorted file with the "sort operation" after the "map
	 * operation"
	 * 
	 */
	public void sortAll() {
		writeInFile();
		actualSize = 0;
		bufferedList.clear();

		BufferedWriter bw = null;
		try {
			File dir = new File(tmpPath);
			File[] files = dir.listFiles();
			ArrayList<HeadFile> fileHeads = initAllReaders(files);
			File sortedFile = new File(dir, "sorted/tmp.txt");
			sortedFile.getParentFile().mkdir();
			bw = new BufferedWriter(new FileWriter(new File(dir, "sorted/tmp.txt")));
			while (otherLines(fileHeads)) {
				String lower = null;
				HeadFile pointer = null;
				// foreach headline select minor and memorize associated
				// HeadFile
				for (HeadFile headFile : fileHeads) {
					if (headFile.fileNotEnded()) {
						String line = headFile.getLastReadedLine().split(" ")[0];
						if (lower == null || line.compareTo(lower) < 0) {
							lower = line;
							pointer = headFile;
						}
					}
				}
				writeSorted(bw, pointer.getLastReadedLine());
				pointer.nextLine();

			}
			writeLastValue(bw);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			try {
				if (bw != null)
					bw.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	private void writeLastValue(BufferedWriter bw) {
		try {
			bw.write(lastReadedKey + " " + lastReadedKeyValues);
			bw.write("\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String lastReadedKey = null;
	private String lastReadedKeyValues = "";

	private void writeSorted(BufferedWriter bw, String lastReadedLine) {
		try {
			String[] values = lastReadedLine.split(" ", 2);
			if (lastReadedKey == null)
				lastReadedKey = values[0];

			if (!values[0].equals(lastReadedKey)) {
				bw.write(lastReadedKey + " " + lastReadedKeyValues);
				bw.write("\n");
				lastReadedKey = values[0];
				lastReadedKeyValues = values[1];
			} else {
				lastReadedKeyValues += values[1];
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Check presence of other Lines in some files
	 * 
	 * @param fileHeads
	 * @return
	 */
	private boolean otherLines(ArrayList<HeadFile> fileHeads) {
		for (HeadFile headFile : fileHeads) {
			if (!headFile.endOfFile)
				return true;
		}
		return false;
	}

	private ArrayList<HeadFile> initAllReaders(File[] files) {
		ArrayList<HeadFile> fileHeads;
		// init Reader on all files
		fileHeads = new ArrayList<>();
		for (int i = 0; i < files.length; i++) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(files[i]));
				HeadFile headFile = new HeadFile(reader);
				fileHeads.add(headFile);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		return fileHeads;
	}

	private class HeadFile {
		private BufferedReader reader;
		private String lastReadedLine;
		public boolean endOfFile = false;

		public HeadFile(BufferedReader reader) {
			this.reader = reader;
			nextLine();
		}

		public boolean fileNotEnded() {
			return !endOfFile;
		}

		public String getLastReadedLine() {
			return lastReadedLine;
		}

		public void nextLine() {
			try {
				lastReadedLine = reader.readLine();
				endOfFile = (lastReadedLine == null);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}

	private class MyEntry implements Entry {
		Object key;
		Object value;

		public MyEntry(Object key, Object value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public Object getKey() {
			return this.key;
		}

		@Override
		public Object getValue() {
			return this.value;
		}

		@Override
		public Object setValue(Object value) {
			return this.value = value;
		}

		@Override
		public String toString() {
			return key + "-" + value;
		}

	}

}
