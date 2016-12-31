package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.interaction.Kill;
import it.unipr.aotlab.actodes.runtime.Shutdown;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.action.Reduce;
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
	// Map blocks number to decrement
	private int mapBlocksCount;
	// Reduce blocks number to decrement
	private int reduceBlocksCount;
	// total number of blocks
	private int maxMapBlocksCount;
	// total number of reduce blocks
	private int maxReduceBlocksCount;

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
		mapBlocksCount = fh.countMapBlocks();
		maxMapBlocksCount = mapBlocksCount;

		/******** Response case ********/
		process = (m) -> {
			System.out.println("risposta da " + m.getSender().getName() + ": " + m.getContent());
			this.responseCount++;

			if (mapBlocksCount > 0) {
				// assign another block to this worker
				mapBlocksCount--;
				System.out.println("ask to workers[" + m.getSender().getName() + "] to map");
				future(m, getMapFunction(), process);
			} else if (this.responseCount == this.maxMapBlocksCount) {
				// All workers have ended Map fucntion, start with Reduce function
				this.reduceBlocksCount = fh.countReduceBlocks();
				this.maxReduceBlocksCount = this.reduceBlocksCount;
				currentWorkerIdx = 0;
				// first call to workers
				while (currentWorkerIdx < this.workerNum && this.reduceBlocksCount > 0) {
					reduceBlocksCount--;
					System.out.println("ask to workers[" + this.currentWorkerIdx + "] to reduce");
					future(workers[this.currentWorkerIdx++], getReduceFunction(), process);
				}
			} else if (reduceBlocksCount > 0) {
				// assign another block to reduce to this worker
				reduceBlocksCount--;
				System.out.println("ask to workers[" + m.getSender().getName() + "] to reduce");
				future(m, getReduceFunction(), process);
			} else if (responseCount == this.maxMapBlocksCount + this.maxReduceBlocksCount) {
				// stop application
				for (int i = 0; i < this.workerNum; i++) {
					send(workers[i], Kill.KILL);
				}
				return Shutdown.INSTANCE;
			}

			return null;
		};
		/******** end case ********/

		// first call to workers
		while (currentWorkerIdx < this.workerNum && mapBlocksCount > 0) {
			mapBlocksCount--;
			System.out.println("ask to workers[" + this.currentWorkerIdx + "] to map");
			future(workers[this.currentWorkerIdx++], getMapFunction(), process);
		}

	}

	private Reduce getReduceFunction() {
		return new Reduce();
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
