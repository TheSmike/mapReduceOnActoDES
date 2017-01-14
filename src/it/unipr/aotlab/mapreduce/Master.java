package it.unipr.aotlab.mapreduce;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.Message;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.interaction.Kill;
import it.unipr.aotlab.actodes.runtime.Shutdown;
import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.action.Reduce;
import it.unipr.aotlab.mapreduce.action.Sort;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.ReduceJob;
import it.unipr.aotlab.mapreduce.exception.InitializeException;
import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;
import it.unipr.aotlab.mapreduce.utils.StrUtils;

/**
 * Master is the Initiator of application, it create and controls other Actor
 * and ask to them to make same work. This is a MapReduce envinroment so Master
 * can ask a specific worker to execute a Job. There are 3 kind of job:
 * <ul>
 * <li>{@link MapJob}</li>
 * <li>{@link Sort} job</li>
 * <li>{@link ReduceJob}</li>
 * </ul>
 * . Map and Reduce are defined as parameters, the Sort function is fixed.
 * 
 */
public final class Master extends Behavior {

	private static final DateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS'Z'");
	private static final int BUFFERED_CONTEXT_DEFAULT_SIZE = 10 * 1024; // 10Kb
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
	 *            the number of workers, path of input files, path of output
	 *            files, blockSize of each block, mapJob: the map function,
	 *            reduceJob: the reduce function, buffer Size for temp sorted
	 *            lines.
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
		int bufferedContextSize;
		if (v.length > 6 && v[6] != null)
			bufferedContextSize = (int) v[6];
		else
			bufferedContextSize = BUFFERED_CONTEXT_DEFAULT_SIZE;
		this.rh = new ResourcesHandler(inputPath, outputPath, blockSize, bufferedContextSize);
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
				printTime("START SORT");
				// sortMapResult(workers);
				launchSortWorker(m);
			} else if (this.responseCount == this.maxMapBlocks + 1) {
				printTime("START REDUCE ");
				launchAllReduceWorker(workers);

			} else if (reduceBlocksCount < maxReduceBlocksCount) {
				launchReduceWorker(m);
			} else if (responseCount == this.maxMapBlocks + this.maxReduceBlocksCount + 1) {
				rh.closeReduceContext();
				// rh.deleteTmpFiles();
				printTime("STOP");
				return stopApplication(workers);
			}

			return null;
		};
		/******** end case ********/

		// first call to workers
		printTime("START MAP");
		launchAllMapWorker(workers);

	}

	private void printTime(String text) {
		System.out.println(text + " => " + sdf.format(new Date()));
	}

	/**
	 * 
	 * This method send a kill message at all worker that are alive, after that
	 * application is end.
	 * 
	 * @param workers
	 *            : Reference of workers
	 * @return
	 */
	private Behavior stopApplication(Reference[] workers) {
		// stop application
		for (int i = 0; i < this.workerNum; i++) {
			send(workers[i], Kill.KILL);
		}
		return Shutdown.INSTANCE;
	}

	/**
	 * 
	 * send a message to all worker available to do a map function on a specific
	 * block different for each one.
	 * 
	 * @param workers
	 *            Reference of workers
	 */
	private void launchAllMapWorker(Reference[] workers) {
		while (currentWorkerIdx < this.workerNum && mapBlocksCount < maxMapBlocks) {
			System.out.println("ask to workers[" + this.currentWorkerIdx + "] to map");
			future(workers[this.currentWorkerIdx++], getMapFunction(mapBlocksCount++), process);
		}
	}

	/**
	 * send a message to current worker that just answered that he had finished
	 * previous Map function.
	 * 
	 * @param m
	 *            message of type mapcase
	 */
	private void launchMapWorker(Message m) {
		// assign another block to this worker
		System.out.println("ask to workers[" + m.getSender().getName() + "] to map");
		future(m, getMapFunction(mapBlocksCount++), process);
	}

	/**
	 * 
	 * contact N worker available for N block that are necessary for perform the
	 * reduce function.
	 * 
	 * @param workers
	 *            : Reference of workers
	 */
	private void launchAllReduceWorker(Reference[] workers) {
		// All workers have ended Map fucntion, start with Reduce function
		this.reduceBlocksCount = 0;
		this.maxReduceBlocksCount = rh.countReduceBlocks();
		System.out.println("blocchi per Reduce: " + maxReduceBlocksCount);
		currentWorkerIdx = 0;
		// first call to workers
		while (currentWorkerIdx < this.workerNum && this.reduceBlocksCount < maxReduceBlocksCount) {
			System.out.println("ask to workers[" + this.currentWorkerIdx + "] to reduce");
			future(workers[this.currentWorkerIdx++], getReduceFunction(reduceBlocksCount++), process);
		}
	}

	/**
	 * 
	 * That method send a message to a worker for performing the reduce
	 * function, send a message to current worker that just answered that he had
	 * finished previous Reduce function.
	 * 
	 * @param m
	 *            message for communicate at a worker to make the reduce
	 *            function
	 */
	private void launchReduceWorker(Message m) {
		// assign another block to reduce to this worker
		System.out.println("ask to workers[" + m.getSender().getName() + "] to reduce");
		future(m, getReduceFunction(reduceBlocksCount++), process);
	}

	/**
	 * Ask to Worker to execute reduce function
	 * @param m
	 *            message for communicate at a worker to do a sort function
	 * 
	 */
	private void launchSortWorker(Message m) {
		// ask a worker to make the sort function
		System.out.println("ask to workers[" + m.getSender().getName() + "] to make the sort operation");
		future(m, getSortFunction(), process);
	}

	/**
	 * @param reduceBlock
	 *            number of reduce block
	 * @return a new reduce instance for performing the reduce operation on specified block
	 */
	private Reduce getReduceFunction(int reduceBlock) {
		return new Reduce(this.rh, reduceBlock, this.reduceJob);
	}

	/**
	 * @param mapBlock
	 *            number of map block
	 * @return a new map instance for performing the map operation on specified block
	 */
	private Map getMapFunction(int mapBlock) {
		return new Map(this.rh, mapBlock, this.mapJob);
	}

	/**
	 * @return a new Sort instance for performing the sort operation on files coming from Map function
	 */
	private Sort getSortFunction() {
		return new Sort(this.rh);
	}

	/**
	 * @param v
	 *            : v got 6 arguments (check if argument is 6) v[0] = number of
	 *            workers (check if the number of workers is valid (>=1) v[1] =
	 *            input path that contain file or directory (check if is null)
	 *            v[2] = output path that contain file or directory (check if is
	 *            null) v[3] = size of the block of the map/reduce operation
	 *            (check if is >=1) v[4] = mapJob v[5] = reduceJob
	 * @return
	 */
	private boolean checkInputValidity(Object[] v) {
		try {
			if (v.length < 6)
				throw new InitializeException("At least 6 required parameters for the program");
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
			// v[6] optional
		} catch (InitializeException e) {
			System.err.println(e.getMessage());
			return false;
		}

		return true;
	}

}
