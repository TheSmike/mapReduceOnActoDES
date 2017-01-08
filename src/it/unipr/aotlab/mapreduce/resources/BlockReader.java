package it.unipr.aotlab.mapreduce.resources;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class BlockReader {
	private String inputPath; // folder or file path
	private int totalBlockNumber;
	private int blockSize;
	private List<Integer> startPosition_list;
	protected RandomAccessFile reader;

	//costruttore
	public BlockReader(String inputPath, int blockSize) throws IOException {
		this.inputPath = inputPath;
		this.blockSize = blockSize;
		this.startPosition_list = new ArrayList<Integer>();
		
		//conteggio blocchi
		int dim_file = (int)new File(inputPath).length();
		
		
		int num_blocchi = dim_file/this.blockSize;

		this.totalBlockNumber = num_blocchi;
		
		File fileToRead = new File(this.inputPath);
		
		try {
			this.reader = new RandomAccessFile(fileToRead, "r");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		
		int startPosition = 0;
		int blocchi_di_scarto = 0;
		
		
	    for ( int i = 0; i < totalBlockNumber; i++)
	    {
	    	
	      boolean scarto = false;
	    	
	      reader.seek(startPosition);
	      
	      //System.out.println("startposition:"+startPosition);
	      
	      if(i == 0)
	      {
	      
	    	startPosition = 0;
	    	
	    
	    	 
	      }
	      else
	      {
	    	  //System.out.println("startposition senza blocksize = "+startPosition);
	    	  reader.seek(startPosition+blockSize);
	    	  
	    	 

	  		//salvo il puntatore corrente di lettura file
	  		long pointer = reader.getFilePointer();
	  		
	  		System.out.println("pointer: "+pointer+" - "+fileToRead.length()+" \n");
	  		
	  		if(pointer > fileToRead.length()) 
	  			{
	  			System.out.println("sto per scartare un blocco \n");
	  			pointer = fileToRead.length();
	  			blocchi_di_scarto++;
	  			scarto = true;
	  			reader.seek(pointer);
	  			}
	  		
	  		//System.out.println(pointer);
	  		
	  		
	  		if(pointer != fileToRead.length())
	  		{
	  		//tiene conto che non sono a fine riga
	  		if(reader.readLine().length() != 0)
	  		{
	  		reader.seek(pointer-1);
	  		byte bit = reader.readByte();
	  		char c = (char) bit;
	  		System.out.println("char: "+c+"\n");
	  		if(c != '\n') // controllo di non essere appena a capo
	  		{
	  			//System.out.println("sono a fine riga");
	  		System.out.println("Non sono a fine riga \n");	
	  		
	  		//ho dei byte da leggere per arrivare a fine riga
	  		
	  		reader.seek(pointer);
	  				
	  		String line = reader.readLine();
	  		
	  		System.out.println(line);
	  		
	  		startPosition = startPosition+blockSize+line.length()+1;
	  		//startPosition = startPosition+blockSize+line.length();
	  		}
	  		else
	  		{
	  		System.out.println("Sono a fine riga \n");
	  		startPosition =	startPosition+blockSize;
	  		}
	  		
	  		
	  		
	  		}
	  		
	  		
	  		}
	      }
	      
	      if(scarto == false) startPosition_list.add(startPosition);
	      
	      
	    	
	    } 
		
		
	    for(Integer posizione : startPosition_list)
	    {
	    	System.out.print("Startposition blocchi :"+posizione+"\n");
	    }
	    
	    System.out.println("num blocchi da scartare \n :"+ blocchi_di_scarto + "\n");
	    if( blocchi_di_scarto > 0) this.totalBlockNumber -= blocchi_di_scarto;
	    
	    reader.close();
		
	}

	public int getTotalBlockNumber() {
		return totalBlockNumber;
	}

	public NewLinesReader getInputLinesReader(int blockNumber) throws IOException {
		// todo in base al blockNumber restituisce un oggetto che legge dal byte
		// X al byte Y oppure al blockSize
		//int inizio_lettura = blockNumber*blockSize;
		
		File f = null;
		
		
		
		f = new File(this.inputPath);
		
		//System.out.println("lunghezza_file:"+f.length());
		
		//return new NewLinesReader(blockNumber,blockSize,f);
		return new NewLinesReader(startPosition_list.get(blockNumber), blockSize, f );
		
	}
	
	public static void main(String[] args) throws IOException
	{
		String cartella = "resources/CountWord/";
		
		File cartellaCorrente = new File(cartella);

		File files[]=cartellaCorrente.listFiles();	
		
		for( File f : files ){		

		    String nomeFile = f.getName();
  
                                     
		System.out.println("Elaborazione a blocchi del percorso: "+nomeFile+"\n");
		//}
		
		
		BlockReader blockreader = new BlockReader(cartella+nomeFile,25);
		
		
		//BlockReader blockreader = new BlockReader("resources/CountWord/file_parole.txt",25);
		
		int n_blocchi = blockreader.getTotalBlockNumber();
		
		System.out.println("Il numero di blocchi in cui spezzo il file è "+n_blocchi+" \n");
		
		for(int i = 0; i < blockreader.totalBlockNumber; i++)
		{
		
		 NewLinesReader line_reader = blockreader.getInputLinesReader(i);
		  
		  String line;
		
		  System.out.println("iterazione "+i+"\n");
		  while ((line = line_reader.readLine()) != null) {
		    // esegui elaborazione sulla riga
			 System.out.println("readline: ["+line+"]");
			 //System.out.println(line);
			
			
		  }
		}
		
		System.out.println("Esecuzione terminata \n");
		
		blockreader.reader.close();
		
		}//fine for files
		  
	}
}
