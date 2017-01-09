package it.unipr.aotlab.mapreduce.context;

import java.util.Map;

/**
 * This interface build a structure key - (object) value
 *
 */
public interface Context extends AutoCloseable {

	public void put(Object key, Object value);
	
	public void putAll(Map map);

}
