package org.crawling.reviews;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class GetCookProfileUrls {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader reader = null;
		BufferedWriter cookProfileUrl = null;
		BufferedReader oldCookProfileUrl = null;
		HashSet<String> oldUrl = new HashSet<String>();
		try {
			reader = new BufferedReader(new FileReader(new File(
					"/home/s/saibharath/xmlDataFiles/newRecipeUrlFile.txt")));
			cookProfileUrl = new BufferedWriter(new FileWriter(new File(
					"/home/s/saibharath/xmlDataFiles/newCookProfileUrl.txt")));
			oldCookProfileUrl = new BufferedReader(new FileReader(new File(
					"/home/s/saibharath/xmlDataFiles/cookProfileUrl.txt")));

			String url = "";
			while ((url = oldCookProfileUrl.readLine()) != null) {
				oldUrl.add(url);
			}
			oldCookProfileUrl.close();
			long count = 0;
			while ((url = reader.readLine()) != null) {
				try {
					if (url.contains("personalrecipe")
							|| url.contains("customrecipe")
							|| url.contains("weblink") || oldUrl.contains(url)) {
						System.out.println(url);
						System.out.println(count);
						count++;
						continue;
					}
					HttpClient client = new DefaultHttpClient();
					HttpGet request = null;
					request = new HttpGet(url);
					File responseFile = new File(
							"/home/s/saibharath/xmlDataFiles/responseFile3.html");
					System.out.println("I am sending request");
					BufferedWriter htmlWriter = new BufferedWriter(
							new FileWriter(responseFile));
					HttpResponse response = null;
					response = client.execute(request);
					BufferedReader br = new BufferedReader(
							new InputStreamReader(response.getEntity()
									.getContent()));
					String html = "";
					while ((html = br.readLine()) != null) {
						htmlWriter.write(html + "\n");
					}
					htmlWriter.close();
					br.close();
					Document doc = Jsoup.parse(responseFile, "UTF-8", "");
					System.out.println(url);
					System.out.println(count);
					count++;
					if (doc.getElementById("lblSubmitter")
							.getElementsByTag("a").size() > 0) {
						cookProfileUrl.write(doc.getElementById("lblSubmitter")
								.getElementsByTag("a").get(0).attr("href")
								+ "\n");
					}
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {

		}
		try {
			cookProfileUrl.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}