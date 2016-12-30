package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.interaction.Kill;
import it.unipr.aotlab.actodes.runtime.Shutdown;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.exception.InitializeException;
import it.unipr.aotlab.mapreduce.file.FileHandler;
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
	// index of last invoked worker
	private int currentWorkerIdx = 0;
	// blocks number to decrement
	private int blocksCount;
	// total number of blocks
	private int maxBlocksCount;

	private Case process = null;
	private int workerNum;

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
		this.workerNum = (int) v[0];
		String inputPath = (String) v[1];
		String outputPath = (String) v[2];
		FileHandler fh = new FileHandler(inputPath, outputPath);
		Reference[] workers = new Reference[this.workerNum];
		for (int i = 0; i < this.workerNum; i++) {
			workers[i] = actor(new WordCountWorker());
		}

		// determine how many blocks needed
		blocksCount = fh.countBlocks();
		maxBlocksCount = blocksCount;

		// set case
		process = (m) -> {
			System.out.println("risposta da " + m.getSender().getName() + ": " + m.getContent());
			this.responseCount++;

			if (blocksCount > 0) {
				blocksCount--;
				System.out.println("ask to workers[" + m.getSender().getName() + "]");
				future(m, getMapFunction(), process);
			} else {
				if (responseCount == this.maxBlocksCount) {
					// stop application
					for (int i = 0; i < this.workerNum; i++) {
						send(workers[i], Kill.KILL);
					}

					return Shutdown.INSTANCE;
				}
			}
			return null;
		};

		// first call to workers
		while (currentWorkerIdx < this.workerNum && blocksCount > 0){
//			if (this.currentWorkerIdx >= this.workerNum)
//				this.currentWorkerIdx = 0;
			blocksCount--;
			System.out.println("ask to workers[" + this.currentWorkerIdx + "]");
			future(workers[this.currentWorkerIdx++], getMapFunction(), process);
		}

	}

	private Map getMapFunction() {
		return new Map();
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
