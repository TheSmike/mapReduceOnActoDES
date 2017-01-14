package it.unipr.aotlab.mapreduce.countword;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapJob;

/**
 * 
 * @author Scarpenti 
 * @author Viti 
 */
public class CountWordMap implements MapJob {

	/**
	  * {@inheritDoc}
	  * Count number of single word for each line and write it on Context
	  */
	@Override
	public void execute(String line, Context context) throws Exception {
		
		
		List<String> list = new ArrayList<String>(Arrays.asList(line.split(" ")));
		
		
		
		HashMap <String, Integer> map = new HashMap<String,Integer>();
		
		

		
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
		
		
		map.put(elem,contatore);
		
		}
		else
		{
//		System.out.println("Parola "+elem+" già analizzata");
		}
		
		}
		
		
		
		context.putMap(map);
		
	}

}
