/**
 * 
 */
package it.unipr.aotlab.mapreduce.action;

import it.unipr.aotlab.actodes.interaction.Action;
import it.unipr.aotlab.mapreduce.context.MapOutputContext;
import it.unipr.aotlab.mapreduce.context.ReduceJob;
import it.unipr.aotlab.mapreduce.file.FileHandler;

/**
 * @author Omi087
 *
 */
public class Reduce implements Action {
	
	public Reduce() {
		// TODO Auto-generated constructor stub
	}

	public Reduce(FileHandler fh, int reduceBlock, ReduceJob mapJob, MapOutputContext context) {
		// TODO Auto-generated constructor stub
	}
	
}
