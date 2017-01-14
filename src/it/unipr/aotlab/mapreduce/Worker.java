package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.KillableBehavior;
import it.unipr.aotlab.actodes.filtering.MessagePattern;
import it.unipr.aotlab.actodes.filtering.constraint.IsInstance;
import it.unipr.aotlab.actodes.interaction.Done;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.action.Reduce;
import it.unipr.aotlab.mapreduce.action.Sort;

/**
 *
 * The {@code Worker} class defines a behavior that waits for messages from a
 * {@code Master} actor until it receives a {@code KILL} message.
 *
 * When it happens it kills itself.
 * 
 * Define also a {@code mapCase} , {@code reduceCase} and {@code sortCase} that are build to determinate the map message, 
 * the reduce message or the sort message
 *
 *	This class define also 3 {@link MessagePattern} to associate Case with Type of incoming message
 * @see Master
 *
 **/

public final class Worker extends KillableBehavior {
	
	private static final long serialVersionUID = 1L;
	private static final MessagePattern MAPPATTERN = MessagePattern.contentPattern(new IsInstance(Map.class));
	private static final MessagePattern REDUCEPATTERN = MessagePattern.contentPattern(new IsInstance(Reduce.class));
	private static final MessagePattern SORTPATTERN = MessagePattern.contentPattern(new IsInstance(Sort.class));
	
	
	
	// Map function case.
	private Case mapCase;
	// Reduce function case.
	private Case reduceCase;
	// Sort function case
	private Case sortCase;
	
	
	/**
	 * {@inheritDoc}
	 *
	 * @param v
	 *            empty arguments
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
		
		this.sortCase = (m) -> {
			Sort sort = (Sort) m.getContent();
			try{
				sort.executeBlock();
			} catch (Exception e) {
				System.err.println("error in sorting phase: " + e.getMessage());
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			System.out.println(this.getReference().getName()+ " executed sort phase");
			send(m, Done.DONE);
			return null;
		};
		
		b.bind(KILLPATTERN, this.killCase);
		b.bind(MAPPATTERN, this.mapCase);
		b.bind(REDUCEPATTERN, this.reduceCase);
		b.bind(SORTPATTERN, this.sortCase);
	}
}
