/**
 * 
 */
package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.examples.fibonacci.Fibonacci;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.active.ThreadScheduler;

/**
 * @author Omi087
 *
 */
public class Master extends Behavior {

	@Override
	public void initialize(Binder b, Object[] v) {

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final int number = 5;
		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		c.setScheduler(ThreadScheduler.class.getName());
		c.setCreator(Fibonacci.class.getName());
		c.setArguments(number);
		Controller.INSTANCE.run();
	}

}
