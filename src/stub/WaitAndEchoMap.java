package stub;

import java.util.Random;

import it.unipr.aotlab.mapreduce.action.Map;
import it.unipr.aotlab.mapreduce.file.FileHandler;

public class WaitAndEchoMap extends Map {

	private static Random random = new Random();
	
	
	public WaitAndEchoMap(FileHandler fh, int mapBlock) {
		super(fh, mapBlock);
	}

	@Override
	protected void onExecute(String line) throws Exception {
		Thread.sleep(1000 + random.nextInt(1000));
		System.out.println(line);
	}

}
