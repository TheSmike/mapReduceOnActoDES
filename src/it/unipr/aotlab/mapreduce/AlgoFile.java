package it.unipr.aotlab.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class AlgoFile {
	public static void main(String [] args) throws FileNotFoundException, IOException {

	BufferedReader reader = new BufferedReader(new FileReader("C:/Users/Vittorio/Desktop/file_parole.txt"));
	
	String line = reader.readLine();
	
	ArrayList<String> righe = new ArrayList<String> ();
	//String[] collection = new String[10];
	
	
	
	while(line!=null){
	     System.out.println(line);
	     righe.add(line);
	     line = reader.readLine();
	     //righe.add(line);
	}
	     
	
	reader.close();
	
	System.out.println("Stapo il contenuto della lista \n");
	
	//ogni riga la divido con split in 3 elementi che aggiunto ad un nuovo arraylist
	String parole[] = new String[3];
	ArrayList<String> parole2 = new ArrayList<String> ();
	for(String s : righe)
	{
		//System.out.println(s);
		//int spacePos = s.indexOf(" ");
		//System.out.println(spacePos);
		
		parole = s.split(" ");
		for(int i = 0; i < parole.length; i++)
		{
		parole2.add(parole[i]);
		}
		
		
	}
	
	for(String w : parole2)
	{
		
		System.out.println("Parola :"+w+", con "+w.length()+" caratteri \n");
		
		
	}
	
	stampa_su_file(parole2);
	
	
	
	
	
	
	
}

	private static void stampa_su_file(ArrayList<String> parole2) throws IOException {
		// TODO Auto-generated method stub
		
		File file;
		
		
		file = new File("/c:/Users/Vittorio/Desktop/coppie_chiave_valore.txt");
		
		
		FileWriter fw = new FileWriter(file);
		PrintWriter pw = new PrintWriter(fw);
		
		for(String w : parole2)
		{
			
			//System.out.println("Parola :"+w+", con "+w.length()+" caratteri \n");
			pw.println(w+" "+w.length());
			
		}
			
			
		
		pw.close();
		fw.close();
		
	}

	
}
