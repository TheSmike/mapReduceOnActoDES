package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.active.ThreadPoolScheduler;
import it.unipr.aotlab.mapreduce.context.MapJob;
import stub.WaitAndEchoJob;

public class Boot {
	public static void main(String[] args) {
		//params
		final int workers = 3;	
		final int blockSize = 1024;
		final MapJob mapJob = new WaitAndEchoJob();
		final String inputPath = "resources/stub";
		final String outputPath = "resources/output/";
		
		
		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(Master.class.getName());
		c.setArguments(workers, inputPath, outputPath, blockSize, mapJob);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}
}
