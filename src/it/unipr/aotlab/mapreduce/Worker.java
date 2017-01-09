package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.KillableBehavior;
import it.unipr.aotlab.actodes.filtering.MessagePattern;
import it.unipr.aotlab.actodes.filtering.constraint.IsInstance;
import it.unipr.aotlab.actodes.interaction.Done;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.action.Reduce;

/**
 *
 * The {@code Worker} class defines a behavior that waits for messages from a
 * {@code Master} actor until it receives a {@code KILL} message.
 *
 * When it happens it kills itself.
 * 
 * Define also a {@code mapCase} and {@code reduceCase} that are build to determinate the map message or
 * reduce message
 *
 * @see Master
 *
 **/

public final class Worker extends KillableBehavior {
	
	private static final long serialVersionUID = 1L;
	private static final MessagePattern MAPPATTERN = MessagePattern.contentPattern(new IsInstance(Map.class));
	private static final MessagePattern REDUCEPATTERN = MessagePattern.contentPattern(new IsInstance(Reduce.class));

	// Map function case.
	private Case mapCase;
	// Reduce function case.
	private Case reduceCase;
	
	/**
	 * {@inheritDoc}
	 *
	 * @param v
	 *            the arguments:
	 *
	 *            the number of messages.
	 *
	 **/
	@Override
	public void initialize(final Binder b, final Object[] v) {
		this.mapCase = (m) -> {
			Map map = (Map) m.getContent();
			try {
				map.executeBlock();
			} catch (Exception e) {
				System.err.println("error executing Map function: " + e.getMessage());
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			System.out.println(this.getReference().getName() + " executed map function");
			send(m, Done.DONE);
			return null;
		};

		// risposta nel caso di reduceCase
		this.reduceCase = (m) -> {
			Reduce reduce = (Reduce) m.getContent();
			try {
				reduce.executeBlock();
			} catch (Exception e) {
				System.err.println("error executing Reduce function: " + e.getMessage());
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			System.out.println(this.getReference().getName() + " executed reduce function");
			send(m, Done.DONE);
			return null;
		};
		
		b.bind(KILLPATTERN, this.killCase);
		b.bind(MAPPATTERN, this.mapCase);
		b.bind(REDUCEPATTERN, this.reduceCase);
	}
}
