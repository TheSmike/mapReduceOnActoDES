package it.unipr.aotlab.mapreduce.transformstr;

import java.util.List;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

/**
 * Dummy reduce function of simple cipher algorithm
 */
public class TransformStringReduce implements ReduceJob {

	int righeLette = 0; 
	

	/**
	 * {@inheritDoc}
	 * 
	 * Reduce Phases are the opposite of Map Phases, this phases decode lines encoded in MapJob
	 * 
	 */
	@Override
	public void execute(String key, List<String> values, Context context) throws Exception {
		
		StringBuilder elabLine = new StringBuilder();
		for (String string : values) {
			elabLine.append(string);
			elabLine.append(" ");
		}
		
		
		char[] array = elabLine.toString().toCharArray();
		elabLine = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			elabLine.append((char)(array[i]-3));
		}
		
		array = elabLine.toString().toCharArray();
		elabLine = new StringBuilder();
		for (int i = 1; i < array.length; i+=2) {
			elabLine.append(array[i]);
			elabLine.append(array[i-1]);
		}
		
		elabLine = new StringBuilder(elabLine).reverse();
		
		context.put(key, elabLine.toString().toLowerCase());
		
	}
	

}
