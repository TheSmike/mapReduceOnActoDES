package it.unipr.aotlab.mapreduce.stub;

import java.util.List;
import java.util.Random;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.ReduceJob;

public class WaitAndEchoReduceJob implements ReduceJob{

	@Override
	public void execute(String key, List<String> values, Context context) throws Exception {
		if (Integer.parseInt(key) % 2 != 0)
			context.put(key, values.get(0));		
	}

}
