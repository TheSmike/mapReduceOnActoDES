package it.unipr.aotlab.mapreduce.transformstr;

import it.unipr.aotlab.mapreduce.context.Context;
import it.unipr.aotlab.mapreduce.context.MapJob;

/**
 * Map function of simple cipher algorithm
 */
public class TransformStringMap implements MapJob {

	int index = 0;

	/**
	 * {@inheritDoc}
	 * <br>
	 * Map Phases:
	 * <ul>
	 * <li>Reverse line</li>
	 * <li>toUpper line</li>
	 * <li>switch odd char and even char</li>
	 * <li>Ceaser cipher</li>
	 * <ul>
	 * 
	 * @param line
	 * @param context
	 * @throws Exception
	 */
	@Override
	public void execute(String line, Context context) throws Exception {

		StringBuilder elabLine = new StringBuilder(line).reverse();
		char[] array;

		array = elabLine.toString().toCharArray();
		elabLine = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			if (i % 2 == 0)
				elabLine.append(Character.toUpperCase(array[i]));
			else
				elabLine.append(array[i]);
		}

		// System.out.println(elabLine.toString());
		array = elabLine.toString().toCharArray();
		elabLine = new StringBuilder();
		for (int i = 1; i < array.length; i += 2) {
			elabLine.append(array[i]);
			elabLine.append(array[i - 1]);

		}
		if (array.length % 2 == 1)
			elabLine.append(array[array.length - 1]);

		// System.out.println(elabLine.toString());
		// elabLine.append(array[0]);
		// elabLine.append(array[array.length-1]);

		array = elabLine.toString().toCharArray();
		elabLine = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			elabLine.append((char) (array[i] + 3));
		}

		context.put(String.valueOf(index++), elabLine);

	}

}
