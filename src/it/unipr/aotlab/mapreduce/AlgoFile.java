package it.unipr.aotlab.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map;
import java.util.Map.Entry;

//import it.unipr.aotlab.mapreduce.action.Map;

public class AlgoFile {
	public static void main(String [] args) throws FileNotFoundException, IOException {

	BufferedReader reader = new BufferedReader(new FileReader("resources/file_parole.txt"));
	
	String line = reader.readLine();
	
	ArrayList<String> righe = new ArrayList<String> ();
	//String[] collection = new String[10];
	
	
	
	while(line!=null){
	     System.out.println(line);
	     righe.add(line);
	     line = reader.readLine();
	     //righe.add(line);
	}
	     
	//TreeMap
	reader.close();
	
	System.out.println("Stampo il contenuto della lista \n");
	
	//ogni riga la divido con split in 3 elementi che aggiunto ad un nuovo arraylist
	String parole[] = new String[3];
	ArrayList<String> parole2 = new ArrayList<String> ();
	//ArrayList<String> tmp = new ArrayList <String> ();
	
	
	
	ArrayList <ArrayList <String>> file_parole = new ArrayList<ArrayList<String>> ();
	
	
	for(String s : righe)
	{
		//System.out.println(s);
		//int spacePos = s.indexOf(" ");
		//System.out.println(spacePos);
		ArrayList<String> tmp = new ArrayList <String> ();
		
		parole = s.split(" ");
		
		for(int i = 0; i < parole.length; i++)
		{
		parole2.add(parole[i]);
		tmp.add(parole[i]);
		}
		file_parole.add(tmp);
		
		//System.out.println(parole2);
		//System.out.println(file_parole);
		
	}
	
	/*
	for(String w : parole2)
	{
		
		System.out.println("Parola :"+w+", con "+w.length()+" caratteri \n");
		
		
	}
	*/
	stampa_su_file(parole2);
	
	 TreeMap<String, Integer> parola_occorrenze = new TreeMap<String,Integer>();
	
	//ArrayList <ArrayList <String>> file_parole = new ArrayList<ArrayList<String>> ();
	int i;
	for(ArrayList<String> riga_testo : file_parole)
	{
		i=0;
		for( String single_parola : riga_testo )
		{
			String elem = riga_testo.get(i);
			int contatore = 0;
			for(int ii = 0; ii < riga_testo.size(); ii++)
			{
			//System.out.println(single_parola);
				
				if(riga_testo.get(ii).equals(elem)) contatore++;
			}
			System.out.println("Ho trovato nella riga corrente la parola "+elem+" "+contatore+" volte \n");
			//System.out.println("parola:"+single_parola+"contatore:"+contatore);
			parola_occorrenze.put(single_parola, contatore);
			//System.out.println("Parola :"+w+", con "+w.length()+" caratteri \n");
			//String elem = riga_testo.get(i);
			//if(riga_testo.contains(elem))
			//if(single_parola.equals(elem)) i++;
			//System.out.println(riga_testo.get(i));
			i++;
		
		}
	}
	
	Set<Entry<String, Integer>> set = parola_occorrenze.entrySet();
    Iterator<Entry<String, Integer>> iterator = set.iterator();
    while(iterator.hasNext()) {
       Entry<String, Integer> mentry = iterator.next();
       System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
       System.out.println(mentry.getValue());
    }
	
}

	private static void stampa_su_file(ArrayList<String> parole2) throws IOException {
		// TODO Auto-generated method stub
		
		File file;
		
		
		file = new File("resources/coppie_chiave_valore.txt");
		
		
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
