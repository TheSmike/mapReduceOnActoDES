package it.unipr.aotlab.mapreduce.utils;

/**
 * Utility for String Processing
 * @author Omi087
 *
 */
public class StrUtils {
	
	/**
	 * Check if String is null or empty or composed only by space chars
	 * @param str
	 * @return
	 */

	public static boolean isEmpty(String str){
		return (str == null || str.length() == 0 || str.trim().length() == 0);
	}
	
	/**
	 * Check  if String is NOT null, NOT empty and NOT composed only by space chars
	 * @param str
	 * @return
	 */
	public static boolean isNotEmpty(String str){
		return !isEmpty(str);
	}
}
