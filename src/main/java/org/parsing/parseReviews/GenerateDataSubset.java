package org.parsing.parseReviews;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class GenerateDataSubset {

	/**
	 * @param args
	 */
	private static final int _numberOfReviews  = 2845166;
	private static final int _numberOfSubsetReviews = 300000;
	public static void main(String[] args) {
		try {
			List<String> lines = Files.readAllLines(Paths.get("C:\\Users\\Sai Bharath\\Desktop\\xmlDataFiles\\outputJsonReviews.txt"), Charset.defaultCharset());
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File("ReviewSubset.txt")));
			for (int i = 0; i < _numberOfSubsetReviews; i++) {
				int random = (int)Math.random()*_numberOfReviews;
				writer.write(lines.get(random));
			}
			writer.close();
		} catch (Exception e) {

		}
	}

}
