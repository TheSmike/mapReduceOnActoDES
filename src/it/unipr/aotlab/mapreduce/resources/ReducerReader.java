package it.unipr.aotlab.mapreduce.resources;

public interface ReducerReader extends AutoCloseable {
	
	public SortedLine readLine();

}
