/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

/**
 * @author Omi087
 *
 */
public interface ReduceJob {

	public void execute(String line, Context context)  throws Exception;

}
