package it.unipr.aotlab.mapreduce.file;

public interface ReducerReader extends AutoCloseable {
	
	public SortedLine readLine();

}
