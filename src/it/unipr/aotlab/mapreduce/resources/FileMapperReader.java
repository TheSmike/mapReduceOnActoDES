package it.unipr.aotlab.mapreduce.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileMapperReader implements InputContext {

	// private int blockSize;
	private File fileToRead;
	private BufferedReader reader;
	private List<File> inputFiles;

	public FileMapperReader(String path, int blockSize) {
		this.inputFiles = loadPaths(path);
		this.fileToRead = file;
		reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileToRead));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		// this.blockSize = blockSize;
	}
	
	public FileMapperReader(File file, int blockSize) {
		this.fileToRead = file;
		reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileToRead));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		// this.blockSize = blockSize;
	}

	/* (non-Javadoc)
	 * @see it.unipr.aotlab.mapreduce.file.ILinesReader#readLine()
	 */
	@Override
	public String readLine() {
		try {
			return reader.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	}
	
	private List<File> loadPaths(String path) {
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
