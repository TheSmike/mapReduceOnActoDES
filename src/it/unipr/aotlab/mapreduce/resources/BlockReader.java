package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class BlockReader {
	private String inputPath; // folder or file path
	private int totalBlockNumber;
	private int blockSize;

	//costruttore
	public BlockReader(String inputPath, int blockSize) {
		this.inputPath = inputPath;
		this.blockSize = blockSize;
		
		//conteggio blocchi
		int dim_file = (int)new File(inputPath).length();
		
		
		int num_blocchi = dim_file/this.blockSize;
		// fare conteggio dei blocchi
		// ...
		//

		this.totalBlockNumber = num_blocchi;
	}

	public int getTotalBlockNumber() {
		return totalBlockNumber;
	}

	public NewLinesReader getInputLinesReader(int blockNumber) {
		// todo in base al blockNumber restituisce un oggetto che legge dal byte
		// X al byte Y oppure al blockSize
		int inizio_lettura = blockNumber*blockSize;
		
		File f = null;
		
		f = new File("resources/CountWord/file_parole.txt");
		
		return new NewLinesReader(inizio_lettura,blockSize,f);
		
	}
	
	public static void main(String[] args) throws IOException
	{
		BlockReader blockreader = new BlockReader("resources/CountWord/file_parole.txt",10);
		
		int n_blocchi = blockreader.getTotalBlockNumber();
		
		System.out.println("Il numero di blocchi in cui spezzo il file è "+n_blocchi+" \n");
		
		//for(int i = 0; i < blockreader.totalBlockNumber; i++)
		//{
		//genero un nuovo linereader che legge il blocco 3
		//i = 14 al max perchè parto dal blocco 0
		 NewLinesReader line_reader = blockreader.getInputLinesReader(14);
		  
		  String line;
		
		//  System.out.println("iterazione "+i+"\n");
		  while ((line = line_reader.readLine()) != null) {
		    // esegui elaborazione sulla riga
			  
			 
			
			
		  }
		//}
		  
	}
}
