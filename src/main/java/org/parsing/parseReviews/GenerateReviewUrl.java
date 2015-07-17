package org.parsing.parseReviews;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class GenerateReviewUrl {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedWriter writer = null;
		BufferedReader reader1 = null;
		BufferedReader reader2= null;
		HashSet<String> urlSet=new HashSet<String>();
		try {
			writer =new BufferedWriter(new FileWriter(new File("/homes/saibharath/xmlDataFiles/ReviewUrlsNew1.txt")));
			reader1 = new BufferedReader(new FileReader(new File("/homes/saibharath/xmlDataFiles/reviewUrlFile.txt")));
			reader2 = new BufferedReader(new FileReader(new File("/homes/saibharath/xmlDataFiles/outputReviewUrls.txt")));
			String url="";
			while((url=reader1.readLine())!=null){
				urlSet.add(url);
			}
			reader1.close();
			while((url = reader2.readLine())!=null){
				if(!urlSet.contains(url)){
					writer.write(url+"\n");
				}
			}
			reader2.close();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
