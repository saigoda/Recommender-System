package org.crawling.reviews;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.crawling.models.Cook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;

public class CrawlCookData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader cookProfileUrl = null;
		BufferedWriter cookProfile = null;
		HashSet<String> uniqueUrl = new HashSet<String>();
		try {
			cookProfileUrl = new BufferedReader(new FileReader(new File(
					"/home/s/saibharath/xmlDataFiles/newCookProfileUrl.txt")));
			cookProfile = new BufferedWriter(new FileWriter(new File(
					"/home/s/saibharath/xmlDataFiles/outputCookProfile.txt"),
					true));
			String url = "";
			while ((url = cookProfileUrl.readLine()) != null) {
				if (url.contains("Main.aspx")
						|| url.contains("Trusted-Brands-Recipes-and-Tips")
						|| url.contains("trusted-brands-recipes-and-tips")) {
					continue;
				}
				uniqueUrl.add(url);
			}
			System.out.println("The number of unique urls in the file are:"
					+ uniqueUrl.size());
			long count = 0;
			Iterator<String> uniqueIter = uniqueUrl.iterator();
			while (uniqueIter.hasNext()) {
				try {
					url = uniqueIter.next();
					System.out.println(url);
					System.out.println(count);
					if (url.contains("Main.aspx")
							|| url.contains("Trusted-Brands-Recipes-and-Tips")
							|| url.contains("trusted-brands-recipes-and-tips")) {
						count++;
						continue;
					}
					HttpClient client = new DefaultHttpClient();
					HttpGet request = new HttpGet(url);
					HttpResponse response = client.execute(request);
					File cookProfileFile = new File(
							"/home/s/saibharath/xmlDataFiles/cookProfile.html");
					BufferedWriter cookHtml = new BufferedWriter(
							new FileWriter(cookProfileFile));
					BufferedReader responseHtml = new BufferedReader(
							new InputStreamReader(response.getEntity()
									.getContent()));
					String line = "";
					while ((line = responseHtml.readLine()) != null) {
						cookHtml.write(line + "\n");
					}
					responseHtml.close();
					cookHtml.close();
					Document document = Jsoup.parse(cookProfileFile, "UTF-8",
							"");
					Cook cook = new Cook();
					cook.setName(document
							.getElementById(
									"ctl00_SubHeaderMainPlaceHolder_ucProfileHeader_SectionHeader_h1Feature")
							.text());
					Elements div = document.getElementsByClass("myinfo").get(0)
							.children();
					Element anchor = document
							.getElementById("ctl00_CenterColumnPlaceHolder_lolSharedItemCount");
					if (anchor != null) {
						cook.setNumberOfRecipesInRecipeBox(Integer
								.parseInt(anchor.text()
										.replaceAll("[recipe|recipes]", "")
										.trim()));
					}
					if (div.get(3).text().split(":").length > 1) {
						cook.setExpertise(div.get(3).text().split(":")[1]);
					}
					if (div.get(4).text().split(":").length > 1) {
						StringTokenizer st = new StringTokenizer(div.get(4)
								.text().split(":")[1].trim(), ",");
						List<String> interests = new ArrayList<String>();
						while (st.hasMoreTokens()) {
							interests.add(st.nextToken().trim());
						}
						cook.setCookingInterests(interests);
					}
					if (div.get(5).text().split(":").length > 1) {
						StringTokenizer st = new StringTokenizer(div.get(5)
								.text().split(":")[1].trim(), ",");
						List<String> hobbies = new ArrayList<String>();
						while (st.hasMoreTokens()) {
							hobbies.add(st.nextToken().trim());
						}
						cook.setHobbies(hobbies);
					}
					url = url.replaceAll("profile", "recipes");
					int recipeBoxCount = cook.getNumberOfRecipesInRecipeBox();
					int pageNumber = 1;

					List<String> titles = new ArrayList<String>();
					List<String> types = new ArrayList<String>();
					List<Float> overallRating = new ArrayList<Float>();
					List<Integer> memberRating = new ArrayList<Integer>();
					while (recipeBoxCount > 0) {
						long currentTime = System.currentTimeMillis();
						File recipeBox = new File(
								"/home/s/saibharath/xmlDataFiles/cookRecipeBox.html");
						BufferedWriter recipeBoxWriter = new BufferedWriter(
								new FileWriter(recipeBox));
						String pageUrl = url + "?Page=" + pageNumber;
						request = new HttpGet(pageUrl);
						response = client.execute(request);
						BufferedReader recipeBoxReader = new BufferedReader(
								new InputStreamReader(response.getEntity()
										.getContent()));
						while ((line = recipeBoxReader.readLine()) != null) {
							recipeBoxWriter.write(line + "\n");
						}
						recipeBoxWriter.close();
						document = Jsoup.parse(recipeBox, "UTF-8", "");
						Elements listTitle = document
								.getElementsByClass("recipe-list-title");
						for (int i = 0; i < listTitle.size(); i++) {
							if (i == 0)
								continue;
							titles.add(listTitle.get(i).getElementsByTag("a")
									.get(0).attr("title"));
						}
						Elements listTypes = document
								.getElementsByClass("recipe-list-type");
						for (int i = 0; i < listTypes.size(); i++) {
							if (i == 0)
								continue;
							types.add(listTypes.get(i).text());
						}
						Elements overallRatingList = document
								.getElementsByClass("rating-stars-img");
						for (int i = 0; i < overallRatingList.size(); i++) {
							if (i % 2 == 0) {
								overallRating
										.add(Float
												.parseFloat(overallRatingList
														.get(i)
														.getElementsByTag(
																"meta").size() > 0 ? overallRatingList
														.get(i)
														.getElementsByTag(
																"meta").get(0)
														.attr("content")
														: "0.0"));
							} else {
								memberRating.add(Integer
										.parseInt(overallRatingList.get(i)
												.getElementsByTag("meta")
												.size() > 0 ? overallRatingList
												.get(i)
												.getElementsByTag("meta")
												.get(0).attr("content") : "0"));
							}
						}
						recipeBoxCount -= 10;
						pageNumber++;
						long nowCurrentTime = System.currentTimeMillis();
						while ((nowCurrentTime - currentTime) < 200) {
							nowCurrentTime = System.currentTimeMillis();
						}
					}
					cook.setNamesOfRecipesInRecipeBox(titles);
					cook.setTypesOfRecipesInRecipeBox(types);
					cook.setOverallRatingToTheRecipe(overallRating);
					cook.setRatingGivenByUserToRecipe(memberRating);
					cookProfile.write(new Gson().toJson(cook) + "\n");
					count++;
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}
			cookProfile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
