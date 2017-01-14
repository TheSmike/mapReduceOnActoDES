package it.unipr.aotlab.mapreduce.transformstr;

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
 * Boot for Cipher algorithm
 * @author Omi087
 *
 */
public class BootTransformString {
	
	public static void main(String[] args) {
		// params
		final int workers = 1;
		final int blockSize = 1024 * 1024;
		final int bufferedContextSize = 26 * 1024 * 1024;
		final String inputPath = "resources/CountWordMassive/";
		final String outputPath = "output/TransformString/";
		final MapJob mapJob = new TransformStringMap();
		final ReduceJob reduceJob = new TransformStringReduce();

		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(Master.class.getName());
		c.setArguments(workers, inputPath, outputPath, blockSize, mapJob, reduceJob, bufferedContextSize);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}

}
