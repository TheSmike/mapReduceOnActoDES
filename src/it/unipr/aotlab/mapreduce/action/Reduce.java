/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.ReduceJob;
import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;
import it.unipr.aotlab.mapreduce.resources.SortedLine;
import it.unipr.aotlab.mapreduce.resources.SortedLinesReader;

/**
 * 
 * 
 * This class implement the {@code Action} interface, in particular 
 * the {@code Reduce} Action to read all line in sequence in the form (key, List of values) 
 * and call {@link ReduceJob#execute(String, Context)}
 * method
 */
public class Reduce implements Action {
	
	private ResourcesHandler rh;
	private int reduceBlockNumber;
	private ReduceJob job;
	
	/**
	 * @param rh  specify a resourceHandler for the reduce function.
	 * @param reduceBlockNumber  specify the block number that we need to perform the reduce function
	 * @param job  specify the {@code ReduceJob} for the reduce operation
	 */
	public Reduce(ResourcesHandler rh, int reduceBlockNumber, ReduceJob job) {
		super();
		this.rh = rh;
		this.reduceBlockNumber = reduceBlockNumber;
		this.job = job;
	}

	/**
	 * 
	 * Lunch a job that execute the operation defined by an implementation of {@link MapJob} on lines of LinesReader 
	 * for a specific {@code reduceBlockNumber}
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
