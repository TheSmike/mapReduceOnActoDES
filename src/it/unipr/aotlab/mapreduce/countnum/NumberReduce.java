package it.unipr.aotlab.mapreduce.countnum;

import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

/**
 * This class {@code NumerReduce} implement the reduce function working with numbers.
 * This function consist into create a <key, value> pair where key= sum of numbers
 * in a line and the value = median of the median of previous values.
 *
 */
public class NumberReduce implements ReduceJob {

	@Override
	public void execute(String key, List<String> values, Context context) throws Exception {
		//come funzione di reduce faccio la media delle medie
		
		int subtotale = 0;
		int contatore = 0;
		
		for(String s : values)
		{
			subtotale += Integer.parseInt(s);
			contatore++;
		}
		
		
		
		context.put(key,(subtotale/contatore));
		
	}

}
