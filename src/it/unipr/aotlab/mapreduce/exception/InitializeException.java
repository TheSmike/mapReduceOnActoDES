package it.unipr.aotlab.mapreduce.exception;

/**
 * @author Scarpenti 
 * @author Viti 
 */
public class InitializeException extends MapReduceException {

	/**
	 * Exception thrown on Initialization phase for incorrect parameters
	 * @param string
	 */
	public InitializeException(String string) {
		super(string);
	}

}
