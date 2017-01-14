package it.unipr.aotlab.mapreduce.resources;

import java.util.List;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class SortedLine {

	public final String key;
	public final List<String> values;
	
	/**
	 * Rappresent a line of a File where values are separated by space
	 * @param key The first value of the line 
	 * @param values All others value of the line
	 */
	public SortedLine(String key, List<String> values) {
		this.key = key;
		this.values = values;
	}

}
