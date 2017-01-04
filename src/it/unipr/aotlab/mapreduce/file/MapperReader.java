package it.unipr.aotlab.mapreduce.file;

public interface MapperReader extends AutoCloseable {

	public String readLine();

}