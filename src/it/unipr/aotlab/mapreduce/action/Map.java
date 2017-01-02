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
public class Map implements Action {

	private FileHandler fh;
	private int mapBlock;

	public Map(FileHandler fh, int mapBlock) {

	}

	public boolean execute() {
		LinesReader lr = fh.getLinesReader(mapBlock);
		while (lr.hasNext()) {
			String line = lr.readLine();
			// esegui elaborazione sulla riga
			onExecute(line);
			// fine
		}
		return true;
	}

	void onExecute(String line){
		//	TODO make abstract
	}

}
