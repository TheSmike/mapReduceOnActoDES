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

	public void execute(String key, List<String> values, Context context)  throws Exception;

}
