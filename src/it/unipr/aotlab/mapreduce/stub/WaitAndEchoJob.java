package it.unipr.aotlab.mapreduce.stub;

import java.util.Random;

import it.unipr.aotlab.mapreduce.context.MapJob;
import it.unipr.aotlab.mapreduce.context.Context;

/**
 * Dummy example about Map function
 *
 */
public class WaitAndEchoJob implements MapJob {

	private static Random random = new Random();
	private int i = 0;
	
	@Override
	public void execute(String line, Context context) throws Exception {
		Thread.sleep(1000 + random.nextInt(1000));
		int nextKey = getNextKey(); 
		context.put(nextKey, line.substring(0,3));
		System.out.println(nextKey + " - " + line.substring(0,3));
	}

	private synchronized int getNextKey() {
		return i++;
	}

}
