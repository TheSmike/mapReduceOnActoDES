package prova_reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

//per testare la lettura dei testi linea per linea
public class Prova_reader {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File fileToRead = new File("resources/CountWord/file_parole3.txt");
		RandomAccessFile reader = new RandomAccessFile(fileToRead, "r");
		int startpos = 0;
		int counter = 0;
		String line;
		reader.seek(startpos);
		while((line = reader.readLine()) != null)
		{
		//reader.seek(startpos);
		
		//line = reader.readLine();
		
		
		
		int pointer = (int) reader.getFilePointer();
		
		
		counter = pointer;
		//System.out.print("pointer: "+pointer+" e counter : "+counter+"\n");
		//startpos = startpos+line.length()+1;
		System.out.println("linea letta: ["+line+"] "+pointer+" \n");
		
		reader.seek(counter);
		}

	}

}
