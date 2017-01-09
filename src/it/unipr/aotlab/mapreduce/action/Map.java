/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.resources.InputLinesReader;
import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;

/**
 * @author Omi087
 *
 *
 *this class implement the {@code Action} interface, in particular define
 *a {@code Map} Action
 */
public class Map implements Action {

	private ResourcesHandler rh;
	private int mapBlockNumber;
	private MapJob job;

	/**
	 * @param fh : specify a resourceHandler for the map function
	 * @param mapBlockNumber : specify the block number that we need to perform map function
	 * @param job : specify the {@code MapJob} for the map operation
	 */
	public Map(ResourcesHandler fh, int mapBlockNumber, MapJob job) {
		this.rh = fh;
		this.mapBlockNumber = mapBlockNumber;
		this.job = job;
	}

	/**
	 * 
	 * lunch a job that execute the operation defined in LinesReader for a specific 
	 * {@code mapBlockNumber}
	 * 
	 * @throws Exception
	 */
	public void executeBlock() throws Exception {
		try (InputLinesReader lr = rh.getInputLinesReader(mapBlockNumber)) {
			String line = null;
			while ((line = lr.readLine()) != null) {
				// esegui elaborazione sulla riga
				job.execute(line, rh.getMapContext());
				// fine
			}
			rh.getMapContext().close();
		}
	}

}
