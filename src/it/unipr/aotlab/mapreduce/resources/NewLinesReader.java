package it.unipr.aotlab.mapreduce.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

public class NewLinesReader {
	
	protected RandomAccessFile reader;
	private File fileToRead;
	//private int startPosition;
	private int blockSize;
	private int byte_letti;
	private long pointer;
	private int blockNumber;
	private int startPosition;
	//protected BufferedReader reader;
	
	//imposto un line reaer che legge un certo blocco di file
	public NewLinesReader(int startPosition, int blockSize, File file) throws IOException {
		this.fileToRead = file;
		reader = null;
		//this.startPosition = startPosition;
		this.blockSize = blockSize;
		this.byte_letti = 0;
		this.pointer = 0;
		//this.blockNumber = BlockNumber;
		//this.startPosition = 0;
		this.startPosition = startPosition;
		
		
		try {
			reader = new RandomAccessFile(fileToRead, "r");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		reader.seek(startPosition);
		
		
		/*
		//dimensione fissa se ciascun blocco iniziasse ad inizio riga
		//va aggiornata in base alla startposition.
		int inizio_lettura = blockNumber*blockSize;
		
		//System.out.println("startposition iniziale "+inizio_lettura+"\n");
		
		//String percorso = "resources/CountWord/file_parole.txt";
		try {
			reader = new RandomAccessFile(fileToRead, "r");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		//se non sono al primo blocco devo recuperare i byte dell'iterazione precedente
		if( BlockNumber != 0)
		{
			RandomAccessFile find_prev = new RandomAccessFile(fileToRead, "r");
			
			find_prev.seek(inizio_lettura);
			
			long pointer_prev = find_prev.getFilePointer();
			
			System.out.println("il blocco parte dalla posizione "+pointer_prev);
			
			System.out.println("Leggo i prossimi 25 byte (dim blocco): \n");
			
			byte[] b1 = new byte[25];
			find_prev.read(b1,0,25);
			char[] chars1 = new char[b1.length];
			for(int i=0;i < b1.length;i++){
				chars1[i]=(char)b1[i];
				}
			
			String str1 = String.valueOf(chars1);
			
			System.out.println("blocco intero letto : "+str1);
			
			
			find_prev.seek(inizio_lettura);
			
			if(pointer_prev != fileToRead.length())
			{
			//tiene conto che non sono a fine riga
			if(find_prev.readLine().length() != 0)
			{
			find_prev.seek(pointer_prev-1);
			byte bit_prev = find_prev.readByte();
			char c_prev = (char) bit_prev;
			if(c_prev == '\n') // controllo di non essere appena a capo
			{
				System.out.println("sono a fine riga");
				
			}
			else 
			{
			//ho dei byte da leggere per arrivare a fine riga
			
			find_prev.seek(pointer_prev);
					
			String line_prev = find_prev.readLine();
			
			System.out.println("pezzo per finire la riga: ["+line_prev+"]");
			
			
			
			
			inizio_lettura = inizio_lettura+line_prev.length();
			
			
			
			}
			}
			
			}
		
			find_prev.close();
		}
		
		
		System.out.println("Startposition aggiornata: "+inizio_lettura);
		/*
		RandomAccessFile find_pos = new RandomAccessFile(fileToRead, "r");
		
		find_pos.seek(inizio_lettura+blockSize);
		
		
		
		//salvo il puntatore corrente di lettura file
		long pointer = find_pos.getFilePointer();
		
		System.out.println(pointer);
		
		if(pointer != fileToRead.length())
		{
		//tiene conto che non sono a fine riga
		if(find_pos.readLine().length() != 0)
		{
		find_pos.seek(pointer-1);
		byte bit = find_pos.readByte();
		char c = (char) bit;
		if(c == '\n') // controllo di non essere appena a capo
		{
			System.out.println("sono a fine riga");
			this.startPosition = inizio_lettura+blockSize;
		}
		else 
		{
		//ho dei byte da leggere per arrivare a fine riga
		
		find_pos.seek(pointer);
				
		String line = find_pos.readLine();
		
		System.out.println(line);
		
		this.startPosition = inizio_lettura+blockSize+line.length();
		}
		}
		
		}
		
		System.out.println("Se dovessi leggere il blocco cosi come è mi fermerei a "+(inizio_lettura+blockSize));
		System.out.println("La lettura del prossimo blocco parte da "+this.startPosition+"\n");
		*/
		
		
		/*
		if(blockNumber != 0)
		{
			startPosition = inizio_lettura+2;
		}
		else
		{
			startPosition = inizio_lettura;
		}
		
		reader.seek(startPosition);
		
		//String prova = reader.readLine();
		//System.out.println("stringa di prova :["+prova+"]");
		//find_pos.close();
		//reader.seek(startPosition);
		*/
		
		//reader.seek(startPosition);
		
	}

	//dopo che ho settato il line reader leggo la linea
	public String readLine() throws IOException {
		// implementazione del reader
		
		//mi posiziono all'inizio del blocco e quindi all'inizio della riga (nel caso blocco 0)
		//reader.seek(startPosition);
		/*
		RandomAccessFile find_pos = new RandomAccessFile(fileToRead, "r");
		find_pos.seek(startPosition);
		byte bit = find_pos.readByte();
		char c = (char) bit;
		System.out.println("carattere: ["+c+"]");
		if(c == '\n') // controllo di non essere appena a capo
		{
			System.out.println("sono a fine riga");
		}
		*/
		if(byte_letti >= blockSize) 
			{
			System.out.println("NULL");
			return null;
			}
		
		String riga = reader.readLine();
		
		//pointer = reader.getFilePointer();
		
		//System.out.println("puntatore a fine righa: "+pointer+"\n");
		
		
		//reader.seek(pointer);
		
		
		if(riga != null)
		{
		byte_letti += (riga.length());
		}
		
		
		
			
		return riga;
			
		
		
		
		
		
		/*
		
		try
		{
		
			
		//mi posiziono nella posizione definita dal blocco
		reader.seek(startPosition);
		
		
		System.out.println(blockSize);
		
		
		
		byte[] b = new byte[blockSize];
		reader.read(b,0,blockSize);
		char[] chars = new char[b.length];
		for(int i=0;i < b.length;i++){
			chars[i]=(char)b[i];
			}
		
		String str = String.valueOf(chars);
		
		System.out.println(str);
		
		
		
		//salvo il puntatore corrente di lettura file
		long pointer = reader.getFilePointer();
		
		
		reader.seek(pointer);
		
	
		
		
		
		//controllo di non essere a fine file
		if(pointer != fileToRead.length())
		{
		//tiene conto che non sono a fine riga
		if(reader.readLine().length() != 0)
		{
		reader.seek(pointer-1);
		byte bit = reader.readByte();
		char c = (char) bit;
		if(c == '\n') // controllo di non essere appena a capo
		{
			System.out.println("sono a fine riga");
		}
		else 
		{
			//effettuo la lettura tenendo conto anche dello scarto per fine riga
			System.out.println("non sono a fine riga");
			
			System.out.println(reader.readLine().length());
			reader.seek(pointer);
			//leggo la fine della riga ed estraggo il tutto
			byte[] chunks  = null;
			String line = reader.readLine();
			System.out.println(line+"\n");
			chunks = line.getBytes("UTF-8");
			
            System.out.println(chunks.length);
            
            reader.seek(startPosition);
            
            int dim = blockSize+chunks.length;
            byte[] b_resize = new byte[dim];
    		reader.read(b_resize,0,dim);
    		char[] chars_resize = new char[b_resize.length];
    		for(int i=0;i < b_resize.length;i++){
    			chars_resize[i]=(char)b_resize[i];
    			}
    		
    		String str_resize = String.valueOf(chars_resize);
    		
    		System.out.println(str_resize);
    		
    		
    		pointer = reader.getFilePointer();
    		
    		reader.seek(pointer);
    		
			
		}
		
		}//chiusa if != null
		}
		
		}  
	    catch(IOException exc) { 
	      System.out.println("Error seeking or reading."); 
	    } 
	   
	    try { 
	      reader.close(); 
	    } catch(IOException exc) {
	      System.out.println("Error closing file.");
	    }
	   
		
		//leggo tot byte a partire da x e fino a blocksize
		//reader.read(b,startPosition,blockSize);
		
		//System.out.println(reader);
			
		
		/*
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(new File(inputPath), "r");
			
      //for 
      // raf.seek(blockSize);
      // se sono a metà riga, mi posizione alla fine della riga corrente.
      // 
      //arriva il primo lettore di blocco 1: legge per 1024 + byte per finire la riga.
      // secondo lettore blocco 2: seek(1024 + byte per finire la riga)
      //		puoò leggere da qui fino a (1024 + byte per finire la riga)
      // terzo lettore blocco 3: seek(1024 + byte per finire la riga) + seek(1024 + byte per finire la riga)
      
      
      raf.seek(mapBlock * blockSize);
      
			byte[] b = new byte[blockSize];
			raf.read();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (raf != null)
				try {
					raf.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
		}
		
		
		
		*/
		
		
		
		//return null;
	}
}
