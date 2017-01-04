package it.unipr.aotlab.mapreduce.resources;

import java.util.List;

public class SortedLine {

	String key;
	List<String> values;

	public SortedLine(String key, List<String> values) {
		this.key = key;
		this.values = values;
	}

}
