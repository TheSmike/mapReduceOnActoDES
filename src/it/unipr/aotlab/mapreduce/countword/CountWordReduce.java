package it.unipr.aotlab.mapreduce.countword;

import java.util.ArrayList;
import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapContext;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

public class CountWordReduce implements ReduceJob {

	int righeLette = 0; 
	
	/**
	  * {@inheritDoc}
	  */
	@Override
	public void execute(String key, List<String> values, Context context) throws Exception {
		// TODO Auto-generated method stub
		
		int subtotale = 0;
		for(String s : values)
		{
			subtotale += Integer.parseInt(s);
		}
		
//		System.out.println("Totale count:"+ subtotale);
		
		context.put(key,subtotale);

	}
	
	public static void main(String[] args) throws Exception
	{
		CountWordReduce reduce = new CountWordReduce();
		
		List <Integer> numeri = new ArrayList <Integer>();
		
		numeri.add(1);
		numeri.add(2);
		numeri.add(3);
		numeri.add(4);
		
		List<Integer> oldList = numeri;
		List<String> newList = new ArrayList<String>(oldList.size());
		
				for (Integer myInt : oldList) { 
				  newList.add(String.valueOf(myInt)); 
				}
	Context contesto = new MapContext("output/CountWord/prove/", 10 * 1024);
	
	reduce.execute("gatto",newList,contesto);
		
	}
	

}
