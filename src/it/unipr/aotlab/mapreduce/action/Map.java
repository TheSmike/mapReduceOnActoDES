/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.file.MapperReader;
import it.unipr.aotlab.mapreduce.file.ResourcesHandler;

/**
 * @author Omi087
 *
 */
public class Map implements Action {

	private ResourcesHandler fh;
	private int mapBlock;
	private MapJob job;

	public Map(ResourcesHandler fh, int mapBlock, MapJob job) {
		this.fh = fh;
		this.mapBlock = mapBlock;
		this.job = job;
	}

	public void executeBlock() throws Exception {
		try (MapperReader lr = fh.getLinesReader(mapBlock)) {
			String line = null;
			while ((line = lr.readLine()) != null) {
				// esegui elaborazione sulla riga
				job.execute(line, fh.getMapContext());
				// fine
			}
		}
	}

}
