package it.unipr.aotlab.mapreduce.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LinesReader {

	// private String inputPath;
	// private int mapBlock;
	private int blockSize;
	private int readedByte; 
	private RandomAccessFile raf;

	public LinesReader(String inputPath, int mapBlock, int blockSize) {
		// this.inputPath = inputPath;
		// this.mapBlock = mapBlock;
		this.readedByte = 0;
		this.blockSize = blockSize;

		this.raf = null;
		try {
			raf = new RandomAccessFile(new File(inputPath), "r");
			raf.seek(mapBlock * blockSize);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String readLine() {
		try {
//			byte read;
//			while ( (read = raf.readL()) )
			return raf.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		if (raf != null)
			try {
				raf.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	}

	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}
}
