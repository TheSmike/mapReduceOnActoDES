/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.MapOutputContext;
import it.unipr.aotlab.mapreduce.file.FileHandler;
import it.unipr.aotlab.mapreduce.file.LinesReader;

/**
 * @author Omi087
 *
 */
public class Map implements Action {

	private FileHandler fh;
	private int mapBlock;
	private MapJob job;
	private MapOutputContext context;

	public Map(FileHandler fh, int mapBlock, MapJob job, MapOutputContext context) {
		this.fh = fh;
		this.mapBlock = mapBlock;
		this.job = job;
		this.context = context;
	}

	public void executeBlock() throws Exception {
		try (LinesReader lr = fh.getLinesReader(mapBlock)) {
			String line = null;
			while ((line = lr.readLine()) != null) {
				// esegui elaborazione sulla riga
				job.execute(line, context);
				// fine
			}
		}
	}

}
