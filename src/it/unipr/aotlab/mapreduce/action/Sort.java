package it.unipr.aotlab.mapreduce.action;


import it.unipr.aotlab.mapreduce.resources.ResourcesHandler;

/**
 * This class handle and let the sort phase begin, using the {@code ResourcesHandler}
 * with {@code SortAndGroup} method
 *
 */
public class Sort {

	private ResourcesHandler rh;
	//private final Context mapOutputContext;
	//private int mapBlockNumber;
	//private MapJob job;

	
	/**
	 * Class Constructor
	 * 
	 * @param fh = ResourcesHandler
	 */
	public Sort(ResourcesHandler fh) {
		this.rh = fh;
		
	}

	/**
	 * 
	 * execute the {@link ResourcesHandler#sortAndGroup()} method
	 * 
	 * @throws Exception
	 */
	
	public void executeBlock() throws Exception {
		rh.sortAndGroup();
		}
	
	
	
	
}
