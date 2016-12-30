package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.interaction.Kill;
import it.unipr.aotlab.actodes.runtime.Shutdown;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.exception.InitializeException;
import it.unipr.aotlab.mapreduce.utils.StrUtils;

/**
 * The {@code Initiator1} class defines a behavior that creates an
 * {@code EmptyBuffer} actor and a set of (a {@code Producer} and a
 * {@code Consumer}) actors.
 *
 * After a fixed period of time it asks them to kill themselves.
 *
 * @author Omi087
 *
 */
public final class WordCountMaster extends Behavior {

	// Current number of response.
	private int responseCount;

	/**
	 * {@inheritDoc}
	 *
	 * @param v
	 *            the arguments:
	 *
	 *            the number of workers, input path, output path, TODO map
	 *            function, TODO reduce function.
	 *
	 **/
	@Override
	public void initialize(Binder b, Object[] v) {
		if (!checkInputValidity(v)) {
			System.err.println("Fine programma");
			return;
		}

		// Initialize
		int workerNum = (int) v[0];
		String inputPath = (String) v[1];
		String outputPath = (String) v[2];

		Reference[] workers = new Reference[workerNum];
		for (int i = 0; i < workerNum; i++) {
			workers[i] = actor(new WordCountWorker());
		}
		int currentWorkerIdx = 0;
		this.responseCount = 0;
		// determine how many blocks needed
		// TODO

		// set case
		Case process = (m) -> {
			System.out.println("risposta da " + m.getSender().getName() + ": " + m.getContent());
			this.responseCount++;

			if (responseCount == workerNum) {
				// stop application
				for (int i = 0; i < workerNum; i++) {
					send(workers[i], Kill.KILL);
				}

				return Shutdown.INSTANCE;
			}else{
				return null;
			}

		};

		// first call to workers
		for (int i = 0; i < workerNum; i++) {
			future(workers[i], new Map(), process);
		}

	}

	private boolean checkInputValidity(Object[] v) {
		try {
			if (v.length != 3)
				throw new InitializeException("3 required parameters for the program");
			if ((int) v[0] <= 0)
				throw new InitializeException("You need to specify minimum of 1 worker");
			if (StrUtils.isEmpty((String) v[1]))
				throw new InitializeException("Input path is null");
			if (StrUtils.isEmpty((String) v[2]))
				throw new InitializeException("Output path is null");
		} catch (InitializeException e) {
			System.err.println(e.getMessage());
			return false;
		}

		return true;
	}

}
