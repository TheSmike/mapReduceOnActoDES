package it.unipr.aotlab.mapreduce.countnum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapJob;

public class NumberMap implements MapJob {

	@Override
	public void execute(String line, Context context) throws Exception {
		// TODO Auto-generated method stub
		
		//faccio un operazione con i numeri e lo inserisco nel context, potrebbe
		//essere ad esempio sommo i numeri di ogni riga (key = somma) e faccio la media
		//degli stessi (value = media)
		
		List<String> list = new ArrayList<String>(Arrays.asList(line.split(" ")));
		
		List<Integer> numbers = new ArrayList<Integer>();
		
		//coppie intero (somma numeri) - intero (media della somma)
		HashMap <String, Integer> map = new HashMap<String ,Integer>();
		
		//ALGO:
		//1) converto ciascuna parola in numero
		
		for( String s : list)
		{
		
			//prelevo la stringa la converto in numero e metto tutto nella lista
			int numero = Integer.parseInt(s);
			
			numbers.add(numero);
		}
		
		//2) nella lista ho tutti i numeri di una stringa ne formo quindi una coppia
		//chiave valore
		
		int somma = 0;
		int contatore = 0;
		
		for(Integer i : numbers)
		{
			
		somma += i;
		contatore++;
			
		}
		
		//3) genero la coppia chiave valore (somma-media)
		
		
		
		
		map.put(String.valueOf(somma),somma/contatore);
		
		
		
		
		
		context.putAll(map);
		
		
		
		
		
		
	}

}
