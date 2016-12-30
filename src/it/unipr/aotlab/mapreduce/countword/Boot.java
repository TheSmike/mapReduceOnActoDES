package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.active.ThreadPoolScheduler;

public class Boot {

	public static void main(String[] args) {
		final int workers = 3;	
		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(WordCountMaster.class.getName());
		c.setArguments(workers, "inputPath", "outputPath");
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}

}
