package it.unipr.aotlab.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
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
	
	
	
	while(line!=null){
	     System.out.println(line);
	     righe.add(line);
	     line = reader.readLine();
	     
	}
	     
	
	reader.close();
	
	System.out.println("Stampo il contenuto della lista \n");
	
	//ogni riga la divido con split in 3 elementi che aggiunto ad un nuovo arraylist
	String parole[] = new String[3];
	ArrayList<String> parole2 = new ArrayList<String> ();
	
	
	
	
	ArrayList <ArrayList <String>> file_parole = new ArrayList<ArrayList<String>> ();
	
	
	for(String s : righe)
	{
		
		ArrayList<String> tmp = new ArrayList <String> ();
		
		parole = s.split(" ");
		
		for(int i = 0; i < parole.length; i++)
		{
		parole2.add(parole[i]);
		tmp.add(parole[i]);
		}
		file_parole.add(tmp);
		

		
	}
	
	
	stampa_su_file(parole2);
	
	 TreeMap<String, List<Integer>> big_container = new TreeMap <String,List<Integer>>();
	 //TreeMap<String, Integer> parola_occorrenze = new TreeMap<String,Integer>();
	 List<String> parole_list = new ArrayList<String>();
	 List<Integer> valori = new ArrayList<Integer> ();
	
	
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
			
				
				if(riga_testo.get(ii).equals(elem)) contatore++;
			}
			//System.out.println("Ho trovato nella riga corrente la parola "+elem+" "+contatore+" volte \n");
			
			parole_list.add(elem);
			valori.add(contatore);
			//parola_occorrenze.put(single_parola, contatore);
			
			//big_container.put(single_parola, contatore);
			
			//Integer elem2 = parola_occorrenze.get(single_parola);
			
			//System.out.println(parola_occorrenze+"\n");
			//System.out.println("Parola :"+w+", con "+w.length()+" caratteri \n");
			//String elem = riga_testo.get(i);
			//if(riga_testo.contains(elem))
			//if(single_parola.equals(elem)) i++;
			//System.out.println(riga_testo.get(i));
			i++;
		
		}
	}
	
	System.out.println(parole_list+"\n");
	System.out.println(valori+"\n");
	
	
	
	
	for(int ww = 0; ww < parole_list.size(); ww++)
	{
	boolean trovato = false;
	String elem = parole_list.get(ww);
	
	
		
	if(big_container.containsKey(elem)) trovato = true;
	
	
	if(trovato == false)
	{
		
	List<Integer> indici = new ArrayList<Integer>();
	
		for(int kk = 0; kk < parole_list.size(); kk++)
		{
			if(parole_list.get(kk).equals(elem)) indici.add(kk);
				
		
		}
		
	List <Integer> tmp = new ArrayList<Integer>();
	
	
	for(int ii = 0; ii < indici.size(); ii++)
	{
	
	int indice = indici.get(ii);
	tmp.add(valori.get(indice));
	
	}
	
	big_container.put(elem,tmp);
	
	
	}
		
	}
	
	System.out.println(big_container+"\n");

	
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
