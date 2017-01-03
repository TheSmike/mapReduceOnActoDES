/**
 * 
 */
package it.unipr.aotlab.mapreduce.context;

/**
 * @author Omi087
 *
 */
public interface MapJob {

	public void execute(String line, MapOutputContext context)  throws Exception;

}
