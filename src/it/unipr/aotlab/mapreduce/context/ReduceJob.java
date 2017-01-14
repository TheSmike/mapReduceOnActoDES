/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

import java.util.List;

import it.unipr.aotlab.mapreduce.action.Reduce;

/**
 * Rappresent a generic Reducing Job to implement, this Interface is used inside {@link Reduce} 
 * for the purpose to execute the specified ReduceJob implemented.
 * @author Scarpenti 
 * @author Viti 
 */
public interface ReduceJob {

	/**
	 * Execute Job on single line, the method implementation must use {@code context} 
	 * to write on output Context.
	 * @param key : represent a particular key extract in the map process for current processed line
	 * @param values : represent a list of values for the specified key 
	 * @param context : specify a context where to write inside the result of current execution
	 * 
	 * @see Context
	 * 
	 * @throws Exception
	 */
	public void execute(String key, List<String> values, Context context)  throws Exception;

}
