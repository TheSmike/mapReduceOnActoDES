package it.unipr.aotlab.mapreduce.context;

import java.util.Map;

/**
 * This interface build a structure key - (object) value,
 * represents an Object where to write the output data of a Map or Reduce function
 *
 */
public interface Context extends AutoCloseable {

	/**
	 * Put one record at a time inside context
	 * @param key
	 * @param value
	 */
	public void put(Object key, Object value);
	
	/**
	 * Put some records at a time inside context
	 * @param map
	 */
	@SuppressWarnings("rawtypes")
	public void putMap(Map map);

}
