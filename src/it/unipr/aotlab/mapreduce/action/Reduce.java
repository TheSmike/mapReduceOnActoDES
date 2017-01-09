/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.ReduceJob;
import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;
import it.unipr.aotlab.mapreduce.resources.SortedLine;
import it.unipr.aotlab.mapreduce.resources.SortedLinesReader;

/**
 * @author Omi087
 *
 */
/**
 * 
 * 
 * This class implement the {@code Action} interface, in particular 
 * the {@code Reduce} action
 * 
 *
 */
public class Reduce implements Action {
	
	private ResourcesHandler rh;
	private int reduceBlockNumber;
	private ReduceJob job;
	
	/**
	 * @param rh : specify a resourceHandler for the reduce function
	 * @param reduceBlockNumber : specify the block number that we need to perform the reduce function
	 * @param job : specify the {@code MapJob} for the reduce operation
	 */
	public Reduce(ResourcesHandler rh, int reduceBlockNumber, ReduceJob job) {
		super();
		this.rh = rh;
		this.reduceBlockNumber = reduceBlockNumber;
		this.job = job;
	}

	/**
	 * 
	 * lunch a job that execute the operation defined in SortedLinesReader for a specific 
	 * {@code reduceBlockNumber}
	 * 
	 * @throws Exception
	 */
	public void executeBlock() throws Exception {
		try (SortedLinesReader slr = rh.getSortedLinesReader(reduceBlockNumber)) {
			SortedLine line = null;
			while ((line = slr.readLine()) != null) {
				// esegui elaborazione sulla riga
				job.execute(line.key, line.values, rh.getReduceContext());
				// fine	
			}
		}
	}
	
}
