package it.unipr.aotlab.mapreduce.resources;

public interface SortReader extends AutoCloseable {

	public MappedLine readLine();

}