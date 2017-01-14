package it.unipr.aotlab.mapreduce.numfilebuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * The class {@code Builder_file_with_numbers} create a random file with size chosen by the
 * user, and fill it with integer numbers, that file is used for the test with map-reduce 
 * case with integer numbers.
 *
 */
public class BuilderFileWithNumbers {

	public static void main(String[] args)throws FileNotFoundException, UnsupportedEncodingException  {
		
		
		    //Size in Gbs of my file that I want
		    double wantedSize = Double.parseDouble(System.getProperty("size", "0.2"));

		    Random random = new Random();
		    int scostamento = 0;
		    boolean file_esistente;
		    
		    File file;
		    
		    do
			{
			file_esistente = false;
			file = new File("output/NumberMassive/AvgNumbers_"+scostamento+".txt");
			if (file.exists())
			{
				file_esistente = true;
				scostamento++;
			}
			}while(file_esistente == true);
		    
		    
		    
		    file.getParentFile().mkdirs();
		    //File file = new File("prova_file/AvgNumbers.txt");
		    long start = System.currentTimeMillis();
		    PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8")), false);
		    int counter = 0;
		    while (true) {
		        String sep = "";
		        
		        for (int i = 0; i < 100; i++) {
		      
		            int number = random.nextInt(1000) + 1;
		            writer.print(sep);
		            writer.print(number);
		            sep = " ";
		        }
		        writer.println();
		        //Check to see if the current size is what we want it to be
		        if (++counter == 20000) {
		            System.out.printf("Size: %.3f GB%n", file.length() / 1e9);
		            if (file.length() >= wantedSize * 1e9) {
		                writer.close();
		                break;
		            } else {
		                counter = 0;
		            }
		        }
		    }
		    long time = System.currentTimeMillis() - start;
		    System.out.printf("Took %.1f seconds to create a file of %.3f GB", time / 1e3, file.length() / 1e9);
		}
		

	}


