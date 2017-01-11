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
import it.unipr.aotlab.mapreduce.countword.CountWordMap;
import it.unipr.aotlab.mapreduce.countword.CountWordReduce;

/**
 * Launch the application with example of mapreduce with numbers.
 *
 */
public class BootMassiveNumber {

	public static void main(String[] args) {
		// params
		final int workers = 3;
		final int blockSize = 25 * 1024 * 1024;
		final int bufferedContextSize = 100 * 1024 * 1024;
		final String inputPath = "output/NumberMassive/";
		final String outputPath = "output/Numbers/";
		final MapJob mapJob = new NumberMap();
		final ReduceJob reduceJob = new NumberReduce();

		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(Master.class.getName());
		c.setArguments(workers, inputPath, outputPath, blockSize, mapJob, reduceJob, bufferedContextSize);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}

}
