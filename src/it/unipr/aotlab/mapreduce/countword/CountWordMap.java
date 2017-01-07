package it.unipr.aotlab.mapreduce.countword;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapJob;

public class CountWordMap implements MapJob {

	@Override
	public void execute(String line, Context context) throws Exception {
		// TODO Auto-generated method stub
		//context.put(key, value);
		//System.out.println(line+"\n");
		
		List<String> list = new ArrayList<String>(Arrays.asList(line.split(" ")));
		
		//HashMap <String, List<Integer>> map = new HashMap<String,List<Integer>>();
		
		HashMap <String, Integer> map = new HashMap<String,Integer>();
		
		
		//splitto la parole di una riga
//		System.out.println(list+"\n");
		
		//conto le occorrenze di ciascuna parola e ne popolo una hasmap locale per riga
		for(int i = 0; i < list.size(); i++)
		{
			
		boolean trovato = false;
		int contatore = 0;
		String elem =  list.get(i);
		
		if(map.containsKey(elem)) trovato = true;
		
		if(trovato == false)
		{
		for(int j = 0; j < list.size(); j++)
		{
			if(list.get(j).equals(elem)) contatore++;

		}
		
		/*
		List<Integer> conteggio;
		
		if (map.containsKey(elem)) {
		    conteggio = map.get(elem);
		  } else {
		    conteggio = new ArrayList<Integer>();;
		  }
		  conteggio.add(contatore);
		  map.put(elem, conteggio);
		 */ 
		
		  
		//context.put(elem,conteggio);
		
//		System.out.println("La parola "+elem+" è stata trovata nella riga "+contatore+" volte \n");
		
		map.put(elem,contatore);
		
		}
		else
		{
//		System.out.println("Parola "+elem+" già analizzata");
		}
		
		}
		
		//System.out.println(map+"\n");
		
		/*
		for ( Entry<String, List<Integer>> entry : map.entrySet()) {
		    String key = entry.getKey();
		    List<Integer> values = entry.getValue();
		    
		    //System.out.println(key+"\n");
		    //System.out.println(values+"\n");
		    
		    context.put(key, values);
		    
		}
		*/
		
		context.putAll(map);
		
	}

}
