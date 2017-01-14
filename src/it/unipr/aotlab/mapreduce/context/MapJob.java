/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

import it.unipr.aotlab.mapreduce.action.Map;

/**
 * Rappresent a generic Mapping Job to implement, this Interface is used inside {@link Map} 
 * for the purpose to execute the specified MapJob implemented.
 * @author Scarpenti 
 * @author Viti 
 */
public interface MapJob {

	/**
	 * execute Job on single line, the method implementation must use {@code context} 
	 * to write on output Context.
	 * @param line : the line that we want to process with a map function
	 * @param context : specify a context where to write inside
	 * 
	 * @see Context
	 * 
	 * @throws Exception
	 */
	public void execute(String line, Context context)  throws Exception;

}
