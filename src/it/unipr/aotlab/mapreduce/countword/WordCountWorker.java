package it.unipr.aotlab.mapreduce.countword;

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

	@Override
	public void initialize(Binder b, Object[] v) {

		// risposta nel caso di mapCase
		this.mapCase = (m) -> {
			System.out.println("Eseguo la funzione di map");
			send(m, Done.DONE);
			return null;
		};

		// risposta nel caso di reduceCase
		this.reduceCase = (m) -> {
			System.out.println("Eseguo la funzione di reduce");
			send(m, Done.DONE);
			return null;
		};

		b.bind(KILLPATTERN, this.killCase);
		b.bind(MAPPATTERN, this.mapCase);
		b.bind(REDUCEPATTERN, this.reduceCase);

	}
}
