package org.parsing.parseReviews;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class ParseRecipeDetails {
	public static void main(String args[]) {
		Document doc = null;
		try {
			doc = Jsoup.parse(new File("xmlDataFiles/recipeDetails.xml"),"UTF-8","");
			File recipeUrlFile = new File("xmlDataFiles/recipeUrlFile.txt");
			BufferedWriter writer = null;
			try {
				writer = new BufferedWriter(new FileWriter(recipeUrlFile));
				for(Element element : doc.getElementsByTag("loc"))
				{
					writer.write(element.text()+"\n");
				}
			} catch (IOException e1) {
				System.out.println("Error writing to file");
				e1.printStackTrace();
			}
			
		} catch (IOException e) {
			System.out.println("error reading file");
			e.printStackTrace();
		}
		
	}
	
}
