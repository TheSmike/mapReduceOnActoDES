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
	private int startPosition;
	private int blockSize;
	//protected BufferedReader reader;
	
	//imposto un line reaer che legge un certo blocco di file
	public NewLinesReader(int startPosition, int blockSize, File file) {
		this.fileToRead = file;
		reader = null;
		this.startPosition = startPosition;
		this.blockSize = blockSize;
		//String percorso = "resources/CountWord/file_parole.txt";
		try {
			reader = new RandomAccessFile(fileToRead, "r");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		
	}

	//dopo che ho settato il line reader leggo la linea
	public String readLine() throws IOException {
		// implementazione del reader
		// TODO
		//RandomAccessFile reader2 = new RandomAccessFile(fileToRead, "r");
		//System.out.println("Bella \n");
		
		try
		{
		
			
		//mi posiziono nella posizione definita dal blocco
		reader.seek(startPosition);
		
		//long pointer = reader.getFilePointer();
		//reader.seek(0);
		System.out.println(blockSize);
		//String s = reader.readUTF();
		
		//System.out.println(s);
		byte[] b = new byte[blockSize];
		reader.read(b,0,blockSize);
		char[] chars = new char[b.length];
		for(int i=0;i < b.length;i++){
			chars[i]=(char)b[i];
			}
		//char[] chars = new char[blockSize];
		//reader.read(chars,0,blockSize);
		String str = String.valueOf(chars);
		
		System.out.println(str);
		
		//System.out.println(str.length());
		
		//salvo il puntatore corrente di lettura file
		long pointer = reader.getFilePointer();
		
		
		reader.seek(pointer);
		
		//System.out.println(reader.readChar());
		
		
		
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
    		//char[] chars = new char[blockSize];
    		//reader.read(chars,0,blockSize);
    		String str_resize = String.valueOf(chars_resize);
    		
    		System.out.println(str_resize);
    		
    		
    		pointer = reader.getFilePointer();
    		
    		reader.seek(pointer);
    		
			
		}
		
		}//chiusa if != null
		}
		
		   /*
			String line;
			//byte[] b = new byte[blockSize];
			boolean ok = false;
			int lunghezza = 0;
			byte[] chunks  = null;
			
	        while (ok == false) {
	            line = reader.readLine();
	            //lunghezza += line.length();
	            System.out.println(line+"\n");
	            System.out.println(lunghezza);
	            chunks = line.getBytes("UTF-8");
	            System.out.println(chunks.length);
	            //reader2.seek(lunghezza);
	            //System.out.println(reader.readChar());
	            ok = true;
	        }
		*/
		// read the first byte and print it
        //System.out.println("" + reader.read());
        
		//long pointer = reader.getFilePointer();
		//leggo un tot di byte
		//byte[] b = new byte[blockSize];
		
		//System.out.println(reader.readUTF());
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
		
		
		
		return null;
	}
}
