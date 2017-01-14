package it.unipr.aotlab.mapreduce.resources.strfilebuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class BuilderFileWithStrings {
	
	private File sourceFile;
	private File destFile;
	
	/**
	 * This class generates a new file destFile, in which append sourceFile X times until choosen dimension is reached
	 * @param sourceFile path of source file 
	 * @param destFile path of destination file 
	 */
	public BuilderFileWithStrings (File sourceFile, File destFile)
	{
		this.sourceFile = sourceFile;
		this.destFile = destFile;	
	}
	
	
	
	
	/**
	 * Create a sourceFile copy into {@code destFile} path
	 * @throws IOException
	 */
	public void copyFile( ) throws IOException {
		
		File sourceFile = this.sourceFile;
		File destFile = this.destFile;
		  if (!destFile.exists()) {
		    destFile.createNewFile();
		  }
		  FileInputStream fIn = null;
		  FileOutputStream fOut = null;
		  FileChannel source = null;
		  FileChannel destination = null;
		  try {
		    fIn = new FileInputStream(sourceFile);
		    source = fIn.getChannel();
		    fOut = new FileOutputStream(destFile);
		    destination = fOut.getChannel();
		    long transfered = 0;
		    long bytes = source.size();
		    while (transfered < bytes) {
		      transfered += destination.transferFrom(source, 0, source.size());
		      destination.position(transfered);
		    }
		  } finally {
		    if (source != null) {
		      source.close();
		    } else if (fIn != null) {
		      fIn.close();
		    }
		    if (destination != null) {
		      destination.close();
		    } else if (fOut != null) {
		      fOut.close();
		    }
		  }
		}
	
	/**
	 * Append copy of {@code sourceFile}  into {@code destFile}  
	 * @throws IOException
	 */
	public void append() throws IOException
	{
		
		RandomAccessFile reader1 = new RandomAccessFile(this.sourceFile, "rw");
		
		List <String> stringhe = new ArrayList <String> ();
		
		String line;
		while( (line = reader1.readLine()) != null)
		{
		
			stringhe.add(line);
			
		}
		
		//FileWriter f1 = new FileWriter(this.destFile, true);
		//BufferedWriter bw = new BufferedWriter(f1);
		
		
		long dim_file = this.destFile.length();
		
		long dim_fileMB = dim_file/ (1024*1024);
		
		
	    FileWriter fw = new FileWriter(this.destFile,true);
	    BufferedWriter bw = new BufferedWriter(fw);
	    
	    int dim_maxMB = 200;
	    
	    while( dim_fileMB < dim_maxMB)
	    {
	    //System.out.println(dim_fileMB);
	    for( String s : stringhe)
	    {
	    bw.newLine();
	    bw.write(s);
	    }
	    
	    dim_fileMB = (this.destFile.length()) / (1024*1024);
	    
	    }
	    //fw.close();
	    bw.close();
	    fw.close();
	    reader1.close();
		
		//RandomAccessFile reader2 = new RandomAccessFile(this.destFile, "w");
		
		//int lunghezza_file = (int) this.destFile.length();
		
		
		
		
		
	}

	/**
	 * Invokation of this class
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		//leggo un file e srivo tutto questo file su un altro tot volte
		
		File file1 = new File("resources/CountWord/file_parole2.txt");
//		File file1 = new File("resources/CountWord/file_parole.txt");
		
		File file2 = new File("resources/Strings/file_stringhe1.txt");
//		File file2 = new File("resources/Strings/file_stringhe3.txt");
		file2.getParentFile().mkdirs();
		
		
		BuilderFileWithStrings bf = new BuilderFileWithStrings(file1,file2);
		
		
		bf.copyFile();
		
		
		System.out.println("File generato correttamente \n");
		
		
		
		bf.append();
		
		System.out.println("Testo copiato nel file effettuato");

	}

}
