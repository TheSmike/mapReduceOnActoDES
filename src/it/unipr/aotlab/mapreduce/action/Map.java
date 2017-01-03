/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.file.FileHandler;
import it.unipr.aotlab.mapreduce.file.LinesReader;

/**
 * @author Omi087
 *
 */
public abstract class Map implements Action {

	private FileHandler fh;
	private int mapBlock;

	public Map(FileHandler fh, int mapBlock) {
		super();
		this.fh = fh;
		this.mapBlock = mapBlock;
	}

	public void execute() throws Exception {
		try (LinesReader lr = fh.getLinesReader(mapBlock)) {
			String line = null;
			while ((line = lr.readLine()) != null) {
				// esegui elaborazione sulla riga
				onExecute(line);
				// fine
			}
		}
	}

	protected abstract void onExecute(String line) throws Exception;

}
