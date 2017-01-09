package it.unipr.aotlab.mapreduce.resources.oldreaders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * 
 * This class define a LineReader which aims to read the file line by line with the limit
 * specified by {@code blockSize}
 *
 */
public abstract class SequentiallyLinesReader implements AutoCloseable {

	private File fileToRead;
	protected BufferedReader reader;

	/**
	 * Class Constructor
	 * 
	 * @param file : input file that we need to read
	 * @param blockSize : limit of the reading operation defined with a blocksize
	 * that specify how may bytes can be read with the FileReader.
	 */
	public SequentiallyLinesReader(File file, int blockSize) {
		this.fileToRead = file;
		reader = null;
		try {
			reader = new BufferedReader(new FileReader(fileToRead));
		} catch (FileNotFoundException e) {
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
