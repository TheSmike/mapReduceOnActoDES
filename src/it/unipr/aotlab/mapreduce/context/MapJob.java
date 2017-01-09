/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

/**
 * @author Omi087
 *
 */
public interface MapJob {

	/**
	 * @param line : the line that we want to process with a map function
	 * @param context : specify a context that define a couple key-value for each word
	 * extract in map function of the line readed.
	 * 
	 * @see Context
	 * 
	 * @throws Exception
	 */
	public void execute(String line, Context context)  throws Exception;

}
