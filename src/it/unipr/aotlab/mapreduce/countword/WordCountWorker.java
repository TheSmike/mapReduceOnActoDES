package it.unipr.aotlab.mapreduce.countword;

import java.util.Random;

import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.KillableBehavior;
import it.unipr.aotlab.actodes.filtering.MessagePattern;
import it.unipr.aotlab.actodes.filtering.constraint.IsInstance;
import it.unipr.aotlab.actodes.interaction.Done;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.action.Reduce;

public class WordCountWorker extends KillableBehavior {

	private static final long serialVersionUID = 1L;

	private static final MessagePattern MAPPATTERN = MessagePattern.contentPattern(new IsInstance(Map.class));
	private static final MessagePattern REDUCEPATTERN = MessagePattern.contentPattern(new IsInstance(Reduce.class));

	// Map function case.
	private Case mapCase;
	// Reduce function case.
	private Case reduceCase;

	private Random random;
	
	@Override
	public void initialize(Binder b, Object[] v) {
		this.random = new Random();
		// risposta nel caso di mapCase
		this.mapCase = (m) -> {
			try {
				Thread.sleep(random.nextInt(1000));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(this.getReference().getName() + " execute map function");
			send(m, Done.DONE);
			return null;
		};

		// risposta nel caso di reduceCase
		this.reduceCase = (m) -> {
			try {
				Thread.sleep(random.nextInt(1000));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(this.getReference().getName() + " execute reduce function");
			send(m, Done.DONE);
			return null;
		};

		b.bind(KILLPATTERN, this.killCase);
		b.bind(MAPPATTERN, this.mapCase);
		b.bind(REDUCEPATTERN, this.reduceCase);

	}
}
