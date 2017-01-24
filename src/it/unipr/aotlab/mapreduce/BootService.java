package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.active.ThreadPoolScheduler;
import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

public class BootService {
	
	private String inputPath;
	private String outputPath;
	private MapJob mapJob;
	private ReduceJob reduceJob;
	private int workers;
	private int blockSize;
	private int bufferedContextSize;



	public BootService(String inputPath, String outputPath, MapJob mapJob, ReduceJob reduceJob) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.mapJob = mapJob;
		this.reduceJob = reduceJob;
		this.workers = 3;
		this.blockSize = 50 * 1024 * 1024;
		this.bufferedContextSize = 50 * 1024 * 1024;
	}
	
	public int getWorkers() {
		return workers;
	}

	public void setWorkers(int workers) {
		this.workers = workers;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public int getBufferedContextSize() {
		return bufferedContextSize;
	}

	public void setBufferedContextSize(int bufferedContextSize) {
		this.bufferedContextSize = bufferedContextSize;
	}
	
	

	public String getInputPath() {
		return inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public MapJob getMapJob() {
		return mapJob;
	}

	public ReduceJob getReduceJob() {
		return reduceJob;
	}

	public void boot (){
		Configuration c = Controller.INSTANCE.getConfiguration();
		c.setScheduler(ThreadPoolScheduler.class.getName());
		c.setCreator(Master.class.getName());
		c.setArguments(workers, inputPath, outputPath, blockSize, mapJob, reduceJob, bufferedContextSize);
		c.setFilter(Logger.ALLLOGS);
		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);
		Controller.INSTANCE.run();
	}
}
