package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.passive.OldScheduler;

public class Boot {

	public static void main(String[] args) {
		final int workers = 3;
		final int stringhe = 5;
		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(OldScheduler.class.getName());
		c.setCreator(WordCountMaster.class.getName());
		c.setArguments(workers, stringhe);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}

}
