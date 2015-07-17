package org.crawling.reviews;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.crawling.models.ReviewDish;
import org.crawling.models.Reviewer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;

public class CrawlRecipes {

	public static void main(String args[]) {

		BufferedReader reader = null;
		File outputRecipes = new File(
				"/home/s/saibharath/xmlDataFiles/outputJsonRecipes.txt");
		BufferedWriter writer = null;
		long count = 0;
		try {
			writer = new BufferedWriter(new FileWriter(outputRecipes, true));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			reader = new BufferedReader(new FileReader(new File(
					"/home/s/saibharath/xmlDataFiles/newRecipeUrlFile.txt")));
			String url = "";
			while ((url = reader.readLine()) != null) {
				if(url.contains("personalrecipe")||url.contains("customrecipe")||url.contains("weblink")){
					System.out.println(url);
					System.out.println(count);
					count++;
					continue;
				}
				HttpClient client = new DefaultHttpClient();
				HttpGet request = null;
				try {
					request = new HttpGet(url);
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				File responseFile = new File(
						"/home/s/saibharath/xmlDataFiles/responseFile1.html");
				BufferedWriter htmlWriter = new BufferedWriter(new FileWriter(
						responseFile));
				HttpResponse response = null;
				try {
					response = client.execute(request);
				} catch (HttpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				BufferedReader br = new BufferedReader(new InputStreamReader(
						response.getEntity().getContent()));
				String html = "";
				try{
				while ((html = br.readLine()) != null) {
					htmlWriter.write(html + "\n");
				}
				htmlWriter.close();
				br.close();
				Document doc = Jsoup.parse(responseFile, "UTF-8", "");
				System.out.println(url);
				System.out.println(count);
				ReviewDish reviewDish = new ReviewDish();
				reviewDish.setName(doc.getElementById("itemTitle").text());
				String numberOfreviews = doc.getElementById("metaReviewCount") != null ? doc
						.getElementById("metaReviewCount").attr("content")
						: doc.getElementsByAttributeValue("itemprop",
								"reviewCount").text();
				reviewDish.setNumberOfReviews(Integer.parseInt(numberOfreviews
						.length() > 0 ? numberOfreviews : "0"));
				reviewDish.setDescription(doc.getElementById("lblDescription")
						.text());
				String serving = doc.getElementById("zoneIngredients").attr(
						"data-originalservings");
				reviewDish
						.setServings(Integer.parseInt(serving.length() > 0 ? serving
								: "0"));
				List<String> ingredientsAmount = new ArrayList<String>();
				List<String> ingredientsName = new ArrayList<String>();
				Reviewer postedBy = new Reviewer();
				postedBy.setName(doc.getElementById("lblSubmitter")
						.getElementsByTag("a").size() > 0 ? doc
						.getElementById("lblSubmitter").getElementsByTag("a")
						.get(0).text() : "");
				reviewDish.setPostedBy(postedBy);
				for (Element classElement : doc
						.getElementsByAttributeValueContaining("class",
								"ingredient-wrap")) {
					for (Element element : classElement.getElementsByTag("li")) {
						String quantity = element
								.getElementById("lblIngAmount") != null ? element
								.getElementById("lblIngAmount").text() : "";
						ingredientsAmount.add(quantity);
						ingredientsName.add(element
								.getElementById("lblIngName").text());
					}
				}
				reviewDish.setIngredientsAmount(ingredientsAmount);
				reviewDish.setIngredientsName(ingredientsName);
				List<String> directions = new ArrayList<String>();
				for (Element element : doc.getElementsByClass("directions")
						.get(0).getElementsByClass("directLeft").get(0)
						.getElementsByTag("ol").get(0).getElementsByTag("li")) {
					directions.add(element.text());
				}
				reviewDish.setDirections(directions);
				writer.write(new Gson().toJson(reviewDish) + "\n");
				count++;
				}catch(Exception e){
					count++;
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
