package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Scarpenti
 * @author Viti {@code LinesReader} class define a special reader that read a
 *         block of file into an input file
 *
 */
public abstract class LinesReader implements AutoCloseable {

	protected RandomAccessFile reader;
	private File fileToRead;
	private int blockSize;
	private int byte_letti;
	private long pointer;

	/**
	 * class define a special reader that read a block of file into an input
	 * file
	 * 
	 * @param startPosition
	 *            : the position where the block start, reading of the file
	 *            start at this position. The reading of the file end when
	 *            {@code blockSize} is achieved
	 * @param blockSize
	 *            : specific the dimension of the block, this is an information
	 *            of how how many bytes a reader of the block can read.
	 * @param file
	 *            : file to read
	 * @throws IOException
	 */
	public LinesReader(int startPosition, int blockSize, File file) {
		this.fileToRead = file;
		reader = null;
		this.blockSize = blockSize;
		this.byte_letti = 0;
		this.pointer = 0;

		try {
			reader = new RandomAccessFile(fileToRead, "r");
			reader.seek(startPosition);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * 
	 * This method read line by line the file and a string is return value, this
	 * value is equal to null if position of the end of block is exceeded after
	 * a line was read
	 * 
	 * @return String that was read or null if end of block is exceeded.
	 * @throws IOException
	 */
	protected String read() throws IOException {
		if (byte_letti >= blockSize) {
			return null;
		}

		pointer = reader.getFilePointer();
		String riga = reader.readLine();
		int pointer2 = (int) reader.getFilePointer();

		if (riga != null) {
			byte_letti += (pointer2 - pointer);
		}

		return riga;

	}

	@Override
	public void close() throws Exception {
		reader.close();
	}
}
