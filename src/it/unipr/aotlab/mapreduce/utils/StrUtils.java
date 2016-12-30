package it.unipr.aotlab.mapreduce.utils;

public class StrUtils {

	public static boolean isEmpty(String str){
		return (str == null || str.length() == 0 || str.trim().length() == 0);
	}
}
