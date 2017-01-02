package it.unipr.aotlab.mapreduce.file;

public class FileHandler {

	private String inputPath;
	private String outputPath;
	private int blockSize;
	
	public FileHandler(String inputPath, String outputPath, int blockSize) {
		super();
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.blockSize = blockSize;
	}

	/**
	 * Count number of blocks in file/directory
	 * @return
	 */
	public int countMapBlocks() {
		// TODO Auto-generated method stub
		return 5;
	}

	public int countReduceBlocks() {
		// TODO Auto-generated method stub
		return 7;
	}

	public LinesReader getLinesReader(int mapBlock) {
		return new LinesReader(inputPath, mapBlock, blockSize);
//		RandomAccessFile raf = null;
//		try {
//			raf = new RandomAccessFile(new File(inputPath), "r");
//			raf.seek(mapBlock * blockSize);
//			byte[] b = new byte[blockSize];
//			raf.read();
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}finally {
//			if (raf != null)
//				try {
//					raf.close();
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//		}
		
	}
	
//	public String getBlock(int mapBlock) {
//		RandomAccessFile raf = null;
//		try {
//			raf = new RandomAccessFile(new File(inputPath), "r");
//			raf.seek(mapBlock * blockSize);
//			byte[] b = new byte[blockSize];
//			raf.readLine();
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}finally {
//			if (raf != null)
//				try {
//					raf.close();
//				} catch (IOException e) {
//					throw new RuntimeException(e);
//				}
//		}
		
//	}

}
