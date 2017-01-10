package it.unipr.aotlab.mapreduce.largefile_builder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class Builder_file {

	public static void main(String[] args)throws FileNotFoundException, UnsupportedEncodingException  {
		// TODO Auto-generated method stub
		
		//final String alphabet = "0123456789ABCDE";
	   // final int N = alphabet.length();

	   // Random r = new Random();

	   // for (int i = 0; i < 50; i++) {
	    	//char c = alphabet.charAt(r.nextInt(N));
	       // System.out.print(alphabet.charAt(r.nextInt(N)));
	  //  }
		    //Size in Gbs of my file that I want
		    double wantedSize = Double.parseDouble(System.getProperty("size", "1.0"));

		    Random random = new Random();
		    File file = new File("C:/Users/Vittorio/workspace/mapReduceOnActoDES/output/prova_file/AvgNumbers.txt");
		    //File file = new File("prova_file/AvgNumbers.txt");
		    long start = System.currentTimeMillis();
		    PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8")), false);
		    int counter = 0;
		    while (true) {
		        String sep = "";
		        //String alphabet = "0123456789ABCDE";
		        //int N = alphabet.length();
		        for (int i = 0; i < 100; i++) {
		        //for (int i = 0; i < 50; i++) {
		        	//char c = alphabet.charAt(r.nextInt(N));
		            int number = random.nextInt(1000) + 1;
		            writer.print(sep);
		            writer.print(number / 1e3);
		            //writer.print(c);
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


