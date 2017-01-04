package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.Message;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.interaction.Kill;
import it.unipr.aotlab.actodes.runtime.Shutdown;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.action.Reduce;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.ReduceJob;
import it.unipr.aotlab.mapreduce.exception.InitializeException;
import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;
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
public final class Master extends Behavior {

	// Current number of response.
	private int responseCount;
	// index of last invoked worker
	private int currentWorkerIdx = 0;
	// Map blocks number to decrement
	private int mapBlocksCount;
	// Reduce blocks number to decrement
	private int reduceBlocksCount;
	// total number of blocks
	private int maxMapBlocks;
	// total number of reduce blocks
	private int maxReduceBlocksCount;

	private Case process = null;
	private int workerNum;
	private MapJob mapJob;
	private ReduceJob reduceJob;

	private ResourcesHandler rh;

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
		int blockSize = (int) v[3];
		this.mapJob = (MapJob) v[4];
		this.reduceJob = (ReduceJob) v[5];
		this.rh = new ResourcesHandler(inputPath, outputPath, blockSize);
		// determine how many blocks needed
		mapBlocksCount = 0;
		maxMapBlocks = rh.countMapBlocks();
		if (maxMapBlocks == 0) {
			System.out.println("Nessun file da elaborare");
			return;
		}
		// Initialize Worker
		Reference[] workers = new Reference[this.workerNum];
		for (int i = 0; i < this.workerNum; i++) {
			workers[i] = actor(new Worker());
		}

		/******** Response case ********/
		process = (m) -> {
			System.out.println("risposta da " + m.getSender().getName() + ": " + m.getContent());
			this.responseCount++;
			if (mapBlocksCount < maxMapBlocks) {
				launchMapWorker(m);
			} else if (this.responseCount == this.maxMapBlocks) {
				sortMapResult(workers);
				launchAllReduceWorker(workers);
			} else if (reduceBlocksCount > 0) {
				launchReduceWorker(m);
			} else if (responseCount == this.maxMapBlocks + this.maxReduceBlocksCount) {
				return stopApplication(workers);
			}

			return null;
		};
		/******** end case ********/

		// first call to workers
		launchAllMapWorker(workers);

	}

	/**** PRIVATE METHOD ****/

	private void sortMapResult(Reference[] workers) {
		System.out.println(rh.getMapContext());
		rh.sortAndGroup();
	}

	private Behavior stopApplication(Reference[] workers) {
		// stop application
		for (int i = 0; i < this.workerNum; i++) {
			send(workers[i], Kill.KILL);
		}
		return Shutdown.INSTANCE;
	}

	private void launchAllMapWorker(Reference[] workers) {
		while (currentWorkerIdx < this.workerNum && mapBlocksCount < maxMapBlocks) {
			System.out.println("ask to workers[" + this.currentWorkerIdx + "] to map");
			future(workers[this.currentWorkerIdx++], getMapFunction(mapBlocksCount++), process);
		}
	}

	private void launchMapWorker(Message m) {
		// assign another block to this worker
		System.out.println("ask to workers[" + m.getSender().getName() + "] to map");
		future(m, getMapFunction(mapBlocksCount++), process);
	}

	private void launchAllReduceWorker(Reference[] workers) {
		// All workers have ended Map fucntion, start with Reduce function
		this.reduceBlocksCount = rh.countReduceBlocks();
		this.maxReduceBlocksCount = this.reduceBlocksCount;
		currentWorkerIdx = 0;
		// first call to workers
		while (currentWorkerIdx < this.workerNum && this.reduceBlocksCount > 0) {
			reduceBlocksCount--;
			System.out.println("ask to workers[" + this.currentWorkerIdx + "] to reduce");
			future(workers[this.currentWorkerIdx++], getReduceFunction(reduceBlocksCount--), process);
		}
	}

	private void launchReduceWorker(Message m) {
		// assign another block to reduce to this worker
		reduceBlocksCount--;
		System.out.println("ask to workers[" + m.getSender().getName() + "] to reduce");
		future(m, getReduceFunction(0), process);
	}

	private Reduce getReduceFunction(int reduceBlock) {
		return new Reduce(this.rh, reduceBlock, this.reduceJob);
	}

	private Map getMapFunction(int mapBlock) {
		return new Map(this.rh, mapBlock, this.mapJob);
	}

	private boolean checkInputValidity(Object[] v) {
		try {
			if (v.length != 6)
				throw new InitializeException("6 required parameters for the program");
			if ((int) v[0] <= 0)
				throw new InitializeException("You need to specify minimum of 1 worker");
			if (StrUtils.isEmpty((String) v[1]))
				throw new InitializeException("Input path is null");
			if (StrUtils.isEmpty((String) v[2]))
				throw new InitializeException("Output path is null");
			if ((int) v[3] <= 0)
				throw new InitializeException("blockSize is null");
			if (v[4] == null && !(v[4] instanceof MapJob))
				throw new InitializeException("expected 5th parameter as MapJob");
			if (v[5] == null && !(v[5] instanceof ReduceJob))
				throw new InitializeException("expected 6th parameter as ReduceJob");
		} catch (InitializeException e) {
			System.err.println(e.getMessage());
			return false;
		}

		return true;
	}

}
