package it.unipr.aotlab.mapreduce.countword;

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
 * Launch the application with files inside the "Countword" folder, this is an example
 * of map-reduce with strings, files in input are very short, so the example is very simple
 *
 */
public class BootCountWord {
	public static void main(String[] args) {
		// params
		final int workers = 3;
		final int blockSize = 10;
		final String inputPath = "resources/CountWord/";
		final String outputPath = "output/CountWord/";
		final MapJob mapJob = new CountWordMap();
		final ReduceJob reduceJob = new CountWordReduce();

		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(Master.class.getName());
		c.setArguments(workers, inputPath, outputPath, blockSize, mapJob, reduceJob);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}
}
