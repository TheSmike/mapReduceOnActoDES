/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

import java.util.List;

/**
 * @author Omi087
 *
 */
public interface ReduceJob {

	/**
	 * @param key : specify a particular key extract in the map process
	 * @param values : specify a list of values for a particular key at the end of
	 * map phase
	 * @param context : context that build a structure key - list of values.
	 * 
	 * @see Context
	 * 
	 * @throws Exception
	 */
	public void execute(String key, List<String> values, Context context)  throws Exception;

}
