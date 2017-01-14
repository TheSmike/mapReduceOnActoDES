package it.unipr.aotlab.mapreduce.countword;

import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

public class CountWordReduce implements ReduceJob {

	int righeLette = 0; 
	
	/**
	  * {@inheritDoc}
	  * Sum all occurence of single word  
	  */
	@Override
	public void execute(String key, List<String> values, Context context) throws Exception {
		int subtotale = 0;
		for(String s : values)
		{
			subtotale += Integer.parseInt(s);
		}
//		System.out.println("Totale count:"+ subtotale);	
		context.put(key,subtotale);
	}
	

}
