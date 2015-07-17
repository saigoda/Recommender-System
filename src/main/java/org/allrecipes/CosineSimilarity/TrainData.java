package org.allrecipes.CosineSimilarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.allrecipes.CosineSimilarity.UserReviewTextCount.UserReviewTextCountMap;
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
import org.crawling.models.ReviewDish;
import org.tartarus.snowball.ext.EnglishStemmer;

import com.google.gson.Gson;

public class TrainData extends Configured implements Tool {

	public static class TrainDataMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static HashMap<String, Integer> userId = new HashMap<String, Integer>();
		private static HashMap<Integer, ReviewDish> recipes = new HashMap<Integer, ReviewDish>();
		// private static int numberOfUsers = 585700;
		private static HashMap<String, Integer> recipeId = new HashMap<String, Integer>();
		private static HashMap<String, Integer> ingredientId = new HashMap<String, Integer>();
		private static HashSet<String> stopWords = new HashSet<String>();

		@Override
		public void configure(JobConf job) {
			readIds("allrecipes/id/UserID/part-r-00000", userId, job);
			readIds("allrecipes/id/RecipeID/part-r-00000", recipeId, job);
			readIds("allrecipes/reviews/ingredient-experiment/ingredient-id/part-00000",
					ingredientId, job);
			try {
				FileSystem fs = FileSystem.get(job);
				String st = "";
				BufferedReader br = new BufferedReader(
						new InputStreamReader(
								fs.open(new Path(
										"allrecipes/recipes/input/outputJsonRecipes.txt"))));
				while ((st = br.readLine()) != null) {
					if (st.trim().length() > 0) {
						ReviewDish dish = new Gson().fromJson(st.trim(),
								ReviewDish.class);
						recipes.put(recipeId.get(dish.getName()), dish);
					}
				}
				br.close();
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						"allrecipes/stopwords/stopwords.txt"))));
				while ((st = br.readLine()) != null) {
					if (st.length() > 0) {
						stopWords.add(st);
					}
				}
				br.close();
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
			String newSplits = "";
			for (String newSplit : input.split("\\s")) {
				if (!stopWords.contains(newSplit)) {
					EnglishStemmer stemmer = new EnglishStemmer();
					stemmer.setCurrent(newSplit);
					stemmer.stem();
					newSplit = stemmer.getCurrent();
					if (newSplit.length() > 1)
						newSplits += newSplit + " ";
				}
			}
			return newSplits.trim();

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
						ReviewDish dish = recipes.get(recipeId.get(review
								.getReviewDish().getName()));
						for (String ingredient : dish.getIngredientsName()) {
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
										if (userId.get(review.getReviewer()
												.getName()) != null)
											output.collect(
													new Text(userId.get(review
															.getReviewer()
															.getName())
															+ ""),
													new Text(ingredientId
															.get(splits) + ""));
									}
								}
							} else {
								ingredient = stemAndTrim(ingredient);
								if (ingredient.length() > 1)
									if (userId.get(review.getReviewer()
											.getName()) != null)
										output.collect(
												new Text(userId.get(review
														.getReviewer()
														.getName())
														+ ""),
												new Text(ingredientId
														.get(ingredient) + ""));
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	public static class TrainDataReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {

			StringBuilder sb = new StringBuilder();
			HashMap<String, Integer> map = new HashMap<String,Integer>();

			while (values.hasNext()) {
				String value = values.next().toString();
				if (map.containsKey(value)) {
					int val = map.get(value);
					map.put(value, ++val);
				} else
					map.put(value, 1);
			}
			PriorityQueue<String> queue = new PriorityQueue<String>(100000,
					new Comparator<String>() {

						@Override
						public int compare(String s1, String s2) {
							int num1 = Integer.parseInt(s1.split(":")[1]);
							int num2 = Integer.parseInt(s2.split(":")[1]);
							return num2 > num1 ? 1 : num1 == num2 ? 0 : -1;
						}
					});
			Iterator<String> keys = map.keySet().iterator();
			while(keys.hasNext()){
				String mapKey = keys.next();
				queue.add(mapKey+":"+map.get(mapKey));
			}
			while(!queue.isEmpty())
				sb.append(queue.remove().split(":")[0]+",");
			output.collect(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new TrainData(),
					args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate test data");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UserReviewTextCountMap.class);
		job.setReducerClass(TrainDataReduce.class);
		// job.setNumMapTasks(20);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
