/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

/**
 * @author Omi087
 *
 */
public interface ReduceJob {

	public void execute(String key, String[] values, Context context)  throws Exception;

}
