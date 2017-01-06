/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.ReduceJob;
import it.unipr.aotlab.mapreduce.resources.InputLinesReader;
import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;
import it.unipr.aotlab.mapreduce.resources.SortedLine;
import it.unipr.aotlab.mapreduce.resources.SortedLinesReader;

/**
 * @author Omi087
 *
 */
public class Reduce implements Action {
	
	private ResourcesHandler rh;
	private int reduceBlockNumber;
	private ReduceJob job;
	
	public Reduce(ResourcesHandler rh, int reduceBlockNumber, ReduceJob job) {
		super();
		this.rh = rh;
		this.reduceBlockNumber = reduceBlockNumber;
		this.job = job;
	}

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
