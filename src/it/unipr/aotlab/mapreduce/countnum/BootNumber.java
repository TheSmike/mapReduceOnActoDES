package it.unipr.aotlab.mapreduce.countnum;

import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.active.ThreadPoolScheduler;
import it.unipr.aotlab.mapreduce.Master;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

/**
 * Launch the application with example of mapreduce with numbers (only a simple file with a few numbers)
 *
 */
public class BootNumber {
	
	public static void main(String[] args) {
		
		final int workers = 3;
		final int blockSize = 10;
		final String inputPath = "resources/Numbers/";
		final String outputPath = "output/Numbers/";
		final MapJob mapJob = new NumberMap();
		final ReduceJob reduceJob = new NumberReduce();

		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(Master.class.getName());
		c.setArguments(workers, inputPath, outputPath, blockSize, mapJob, reduceJob);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}

}
