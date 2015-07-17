package org.crawling.reviews;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.crawling.models.Location;
import org.crawling.models.Review;
import org.crawling.models.ReviewDish;
import org.crawling.models.Reviewer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;

public class CrawlReviews {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader reader = null;
		long count = 0;
		try {
			reader = new BufferedReader(new FileReader(new File(
					"/homes/saibharath/xmlDataFiles/ReviewUrlsNew.txt")));
			FileWriter outputFile = null;
			String url = "";
			try {
				outputFile = new FileWriter(
						new File(
								"/homes/saibharath/xmlDataFiles/outputJsonReviews.txt"),
						true);
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			try {
				while ((url = reader.readLine()) != null) {
					HttpClient client = new DefaultHttpClient();
					HttpGet request = null;
					try {
						request = new HttpGet(url);
						try {
							HttpResponse response = client.execute(request);
							File responseFile = new File(
									"xmlDataFiles/responseFile.html");
							FileWriter writer = new FileWriter(responseFile);
							BufferedReader br = new BufferedReader(
									new InputStreamReader(response.getEntity()
											.getContent()));
							String str = "";
							while ((str = br.readLine()) != null)
								writer.write(str + "\n");
							writer.close();
							br.close();
							Document doc = Jsoup.parse(responseFile, "UTF-8",
									"");
							ReviewDish reviewDish = new ReviewDish();
							reviewDish.setName(doc.getElementsByClass(
									"plaincharacterwrap").size() > 0 ? doc
									.getElementsByClass("plaincharacterwrap")
									.get(0).getElementsByTag("a").get(0).text()
									: "");
							reviewDish.setParaDescription(doc
									.getElementsByAttributeValueContaining(
											"id", "reviews_paraDescription")
									.size() > 0 ? doc
									.getElementsByAttributeValueContaining(
											"id", "reviews_paraDescription")
									.get(0).text() : "");
							for (Element element : doc
									.getElementsByClass("reviews")) {
								Review review = new Review();
								review.setReviewDish(reviewDish);
								review.setRating(Integer.parseInt(element
										.getElementsByAttributeValue(
												"itemprop", "ratingValue")
										.get(0).attributes().get("content")));
								Date date = null;
								try {
									date = new SimpleDateFormat("MMM. d, yyyy",
											Locale.ENGLISH).parse(element
											.getElementsByClass("recreview")
											.get(0).getElementsByClass("date")
											.get(0).text());
								} catch (ParseException e) {
									try {
										date = new SimpleDateFormat(
												"MMM d, yyyy", Locale.ENGLISH)
												.parse(element
														.getElementsByClass(
																"recreview")
														.get(0)
														.getElementsByClass(
																"date").get(0)
														.text());
									} catch (ParseException e1) {
										System.out.println(url);
										e1.printStackTrace();
									}
								}
								if (date != null) {
									review.setReviewDate(date);
								}
								review.setReview(element
										.getElementsByClass(
												"listItemReviewFull").get(0)
										.text());
								Reviewer reviewer = new Reviewer();
								String reviewerName = element
										.getElementsByClass("reviewertd")
										.get(0).getElementsByClass("top")
										.get(0).getElementsByClass("statsdiv")
										.get(0).text();
								reviewer.setName(reviewerName);
								Element reviewerStats = element
										.getElementsByClass("ubox_stats")
										.get(0);
								if (reviewerStats.children().size() > 0) {
									reviewer.setExpertise(reviewerStats
											.getElementsByTag("p").size() > 0 ? reviewerStats
											.getElementsByTag("p").get(0)
											.text()
											.replaceAll("Cooking Level:", "")
											: "");
									String homeLocation = reviewerStats
											.getElementsByAttributeValueContaining(
													"id", "HomeTownArea")
											.size() > 0 ? reviewerStats
											.getElementsByAttributeValueContaining(
													"id", "HomeTownArea")
											.get(0).getElementsByClass("info")
											.get(0).text()
											: " , , ";
									String currentLocation = reviewerStats
											.getElementsByAttributeValueContaining(
													"id", "LivingInArea")
											.size() > 0 ? reviewerStats
											.getElementsByAttributeValueContaining(
													"id", "LivingInArea")
											.get(0).getElementsByClass("info")
											.get(0).text()
											: " , , ";
									Location homeTownLocation = new Location();
									Location currentTownLocation = new Location();
									for (int i = 0; i < homeLocation.split(",").length
											&& i < currentLocation.split(",").length; i++) {
										if (i == 0) {
											homeTownLocation
													.setCity(homeLocation
															.split(",")[i]
															.length() > 0 ? homeLocation
															.split(",")[i] : "");
											currentTownLocation
													.setCity(currentLocation
															.split(",")[i]
															.length() > 0 ? currentLocation
															.split(",")[i] : "");
										} else if (i == 1) {
											homeTownLocation
													.setState(homeLocation
															.split(",")[i]
															.length() > 0 ? homeLocation
															.split(",")[i] : "");
											currentTownLocation
													.setState(currentLocation
															.split(",")[i]
															.length() > 0 ? currentLocation
															.split(",")[i] : "");
										} else {
											homeTownLocation
													.setCountry(homeLocation
															.split(",")[i]
															.length() > 0 ? homeLocation
															.split(",")[i] : "");
											currentTownLocation
													.setCountry(currentLocation
															.split(",")[i]
															.length() > 0 ? currentLocation
															.split(",")[i] : "");
										}
									}
									reviewer.setHomeTown(homeTownLocation);
									reviewer.setCurrentTown(currentTownLocation);
								}
								review.setReviewer(reviewer);
								outputFile.write(new Gson().toJson(review)
										+ "\n");
							}
						} catch (HttpException e) {
							System.out.println(url);
							e.printStackTrace();
						}
					} catch (URISyntaxException e) {
						System.out.println(url);
						e.printStackTrace();
					}
					System.out.println(url);
					System.out.println(count);
					count++;
				}
				outputFile.close();
			} catch (IOException e) {
				System.out.println(url);
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
