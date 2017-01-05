package it.unipr.aotlab.mapreduce.resources;

public class BlockReader {
	private String inputPath; // folder or file path
	private int totalBlockNumber;
	private int blockSize;

	public BlockReader(String inputPath, int blockSize) {
		this.inputPath = inputPath;
		this.blockSize = blockSize;
		// fare conteggio dei blocchi
		// ...
		int valoreCalcolato = 0; //TODO mettere vero valore
		//

		this.totalBlockNumber = valoreCalcolato;
	}

	public int getTotalBlockNumber() {
		return totalBlockNumber;
	}

	public NewLinesReader getInputLinesReader(int blockNumber) {
		// todo in base al blockNumber restituisce un oggetto che legge dal byte
		// X al byte Y oppure al blockSize
		int X = 0;
		return new NewLinesReader(X, blockSize);
	}
}
