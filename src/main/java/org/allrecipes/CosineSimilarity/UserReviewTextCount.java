package org.allrecipes.CosineSimilarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.crawling.models.Review;
import org.tartarus.snowball.ext.EnglishStemmer;

import com.google.gson.Gson;

public class UserReviewTextCount extends Configured implements Tool {

	public static class UserReviewTextCountMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, Integer> userId = new HashMap<String, Integer>();
		private static HashMap<String, Integer> ingredientId = new HashMap<String, Integer>();
		// private static int numberOfUsers = 585700;
		private static HashSet<String> stopWords = new HashSet<String>();
		private static HashSet<String> dictionary = new HashSet<String>();

		@Override
		public void configure(JobConf job) {
			readIds("allrecipes/id/UserID/part-r-00000", userId, job);
			readIds("allrecipes/reviews/review-text-id/part-00000",
					ingredientId, job);
			try {
				FileSystem fs = FileSystem.get(job);
				String st = "";
				BufferedReader br = new BufferedReader(
						new InputStreamReader(fs.open(new Path(
								"allrecipes/stopwords/stopwords.txt"))));
				while ((st = br.readLine()) != null) {
					if (st.length() > 0) {
						stopWords.add(st);
					}
				}
				br.close();
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						"allrecipes/dictionary/dictionary.txt"))));
				while ((st = br.readLine()) != null) {
					if (st.length() > 0) {
						dictionary.add(st);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void readIds(String path, HashMap<String, Integer> map,
				JobConf job) {
			String st = "";
			try {
				FileSystem fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(path))));
				while ((st = br.readLine()) != null) {
					if (st.trim().length() > 0) {
						String[] splits = st.trim().split("\\t");
						map.put(splits[1], Integer.parseInt(splits[0]));
					}
				}
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public static String stemAndTrim(String input) {
			String newSplit = input;
			if (!stopWords.contains(newSplit)) {
				EnglishStemmer stemmer = new EnglishStemmer();
				stemmer.setCurrent(newSplit);
				stemmer.stem();
				newSplit = stemmer.getCurrent();
				if (!stopWords.contains(newSplit))
					return newSplit.length() > 1 ? newSplit.trim() : "";
				else
					return "";
			} else
				return "";

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			try {
				if (text.length() > 0) {
					Review review = new Gson().fromJson(text, Review.class);
					if (review.getRating() >= 3) {
						String reviewText = review.getReview().trim();
						for (String ingredient : reviewText.split("\\s")) {
							ingredient = ingredient.replaceAll("[^a-zA-Z\\s]",
									"");
							ingredient = ingredient.replaceAll("\\t", "");
							ingredient = ingredient.trim();
							ingredient = ingredient.toLowerCase();
							if (ingredient.contains(" or")) {
								List<String> ingredientSplits = new ArrayList<String>();
								ingredientSplits.add(ingredient.substring(0,
										ingredient.indexOf(" or")));
								ingredientSplits.add(ingredient.substring(
										ingredient.indexOf(" or") + 3,
										ingredient.length()));
								for (String splits : ingredientSplits) {
									// ingredientSplits.remove(splits);
									splits = stemAndTrim(splits);
									if (splits.length() > 1) {
										if (dictionary.contains(splits))
											if (userId.get(review.getReviewer()
													.getName()) != null)
												output.collect(
														new Text(
																userId.get(review
																		.getReviewer()
																		.getName())
																		+ ""),
														new Text(
																ingredientId
																		.get(ingredient)
																		+ ""));
									}
								}
							} else {
								ingredient = stemAndTrim(ingredient);
								if (ingredient.length() > 1)
									if (dictionary.contains(ingredient))
										if (userId.get(review.getReviewer()
												.getName()) != null)
											output.collect(
													new Text(userId.get(review
															.getReviewer()
															.getName())
															+ ""),
													new Text(ingredientId
															.get(ingredient)
															+ ""));
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public static class UserReviewTextCountReduce extends
			MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			HashMap<String, Integer> ingredients = new HashMap<String, Integer>();
			// StringBuffer buffer = new StringBuffer();
			while (values.hasNext()) {
				String ingredient = values.next().toString();
				if (ingredients.containsKey(ingredient)) {
					int val = ingredients.get(ingredient);
					val++;
					ingredients.put(ingredient, val);
				} else {
					ingredients.put(ingredient, 1);
				}
			}
			Iterator<String> keys = ingredients.keySet().iterator();
			while (keys.hasNext()) {
				String ingKey = keys.next();
				output.collect(new Text(ingKey + "," + key.toString() + ","
						+ ingredients.get(ingKey)), new Text(""));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Review Text Count");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UserReviewTextCountMap.class);
		job.setReducerClass(UserReviewTextCountReduce.class);
		// job.setNumMapTasks(1);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Configuration(), new UserReviewTextCount(), args);
	}
}
