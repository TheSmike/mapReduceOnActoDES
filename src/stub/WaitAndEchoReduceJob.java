package stub;

import java.util.List;
import java.util.Random;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

public class WaitAndEchoReduceJob implements ReduceJob{

	private static Random random = new Random();
	private int i = 0;
	
	private synchronized int getNextKey() {
		return i++;
	}

	@Override
	public void execute(String key, List<String> values, Context context) throws Exception {
		Thread.sleep(1000 + random.nextInt(1000));
		int nextKey = getNextKey(); 
		context.put(nextKey, key.substring(0,3));
		System.out.println("reduce " + nextKey + " - " + key.substring(0,3));
		
	}

}
