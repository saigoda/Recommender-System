package org.allrecipes.CosineSimilarity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

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
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.crawling.models.Review;
import org.crawling.models.ReviewDish;
import org.tartarus.snowball.ext.EnglishStemmer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;

public class UserUserSimilarityBasedOnIngredient extends Configured implements
		Tool {

	/**
	 * @param args
	 */

	public static class ReviewTextIdMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, Integer> userId = new HashMap<String, Integer>();
		private static HashMap<Integer, ReviewDish> recipes = new HashMap<Integer, ReviewDish>();
		// private static int numberOfUsers = 585700;
		private static HashMap<String, Integer> recipeId = new HashMap<String, Integer>();
		private static HashSet<String> stopWords = new HashSet<String>();
		private static HashSet<String> dictionary = new HashSet<String>();

		@Override
		public void configure(JobConf job) {
			readIds("allrecipes/id/UserID/part-r-00000", userId, job);
			readIds("allrecipes/id/RecipeID/part-r-00000", recipeId, job);
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
											output.collect(new Text(splits),
													new Text(""));
									}
								}
							} else {
								ingredient = stemAndTrim(ingredient);
								if (ingredient.length() > 1)
									if (dictionary.contains(ingredient))
										output.collect(new Text(ingredient),
												new Text(""));
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public static class IngredientIdMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, Integer> userId = new HashMap<String, Integer>();
		private static HashMap<Integer, ReviewDish> recipes = new HashMap<Integer, ReviewDish>();
		// private static int numberOfUsers = 585700;
		private static HashMap<String, Integer> recipeId = new HashMap<String, Integer>();
		private static HashSet<String> stopWords = new HashSet<String>();

		@Override
		public void configure(JobConf job) {
			readIds("allrecipes/id/UserID/part-r-00000", userId, job);
			readIds("allrecipes/id/RecipeID/part-r-00000", recipeId, job);
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
										output.collect(new Text(splits),
												new Text(""));
									}
								}
							} else {
								ingredient = stemAndTrim(ingredient);
								if (ingredient.length() > 1)
									output.collect(new Text(ingredient),
											new Text(""));
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public static class IngredientIdReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public static enum Ingredient_Counter {
			IngredientCounter;

		};

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			if (reporter.getCounter(Ingredient_Counter.IngredientCounter)
					.getValue() == 0)
				reporter.getCounter(Ingredient_Counter.IngredientCounter)
						.increment(1);
			output.collect(
					new Text(reporter.getCounter(
							Ingredient_Counter.IngredientCounter).getValue()
							+ ""), key);
			reporter.getCounter(Ingredient_Counter.IngredientCounter)
					.increment(1);
		}

	}

	public static class TransformMahoutInputMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, Integer> ingredientId = new HashMap<String, Integer>();

		@Override
		public void configure(JobConf job) {
			readIds(job.get("ingredient") + "/part-00000", ingredientId, job);
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

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split(",");
			output.collect(new Text(ingredientId.get(splits[0]) + ","
					+ splits[1] + "," + splits[2]), new Text(""));
		}

	}

	public static class UserUserSimilarityBasedOnIngredientMap extends
			MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private static HashMap<String, Integer> userId = new HashMap<String, Integer>();
		private static HashMap<Integer, ReviewDish> recipes = new HashMap<Integer, ReviewDish>();
		// private static int numberOfUsers = 585700;
		private static HashMap<String, Integer> recipeId = new HashMap<String, Integer> ();
		private static HashSet<String> stopWords = new HashSet<String>();

		@Override
		public void configure(JobConf job) {
			readIds("allrecipes/id/UserID/part-r-00000", userId, job);
			readIds("allrecipes/id/RecipeID/part-r-00000", recipeId, job);
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

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
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
															+ ""), new Text(
															splits));
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
														+ ""), new Text(
														ingredient));
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class UserUserSimilarityBasedOnIngredientReduce extends
			MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			HashMap<String, Integer> ingredients = new HashMap<String, Integer> ();
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

	public static class InvertedIndexMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String value = values.toString();
			String[] splits = value.split("\\t");
			String[] ingredients = splits[1].split(",");
			for (String ingredient : ingredients) {
				String[] ingNum = ingredient.split(":");
				if (ingNum.length == 2)
					output.collect(new Text(ingNum[0]), new Text(splits[0]
							+ ":" + ingNum[1]));
			}
		}

	}

	public static class InvertedIndexReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private FileSystem fs;
		private Boolean createFiles;
		private String path;

		@Override
		public void configure(JobConf job) {
			try {
				fs = FileSystem.get(job);
				createFiles = Boolean.valueOf(job.get("createFiles"));
				path = job.get("output");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void createFile(String key, StringBuilder value)
				throws IOException {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					fs.create(new Path(path + "/" + key))));
			bw.write((key + ":" + value.toString().split(",").length) + "\t"
					+ value.toString() + "\n");
			bw.close();
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			HashSet<String> ingredients = new HashSet<String>();
			while (values.hasNext()) {
				ingredients.add(values.next().toString());
			}
			StringBuilder sb = new StringBuilder();
			Iterator<String> it = ingredients.iterator();
			while (it.hasNext()) {
				sb.append(it.next() + ",");
			}
			if (createFiles) {
				createFile(key.toString().split(":")[0], sb);
			} else {
				output.collect(
						new Text(key.toString() + ":" + ingredients.size()),
						new Text(sb.toString()));
			}
		}

	}

	public static class ComputeWeightsMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static Multimap<String, String> index = ArrayListMultimap
				.create();
		private static int numberOfUsers = 247859;

		@Override
		public void configure(JobConf job) {
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(fs.open(new Path(job
								.get("indexPath") + "/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					String key = splits[0].split(":")[0];
					String[] ingredients = splits[1].split(",");
					for (String ingredient : ingredients) {
						index.put(key, ingredient);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			int max = Integer.MIN_VALUE;
			for (String s : splits[1].split(",")) {
				String[] ingNum = s.split(":");
				int num = Integer.parseInt(ingNum[1]);
				if (num > max) {
					max = num;
				}
			}
			if (max == Integer.MIN_VALUE) {
				max = 0;
			}
			double tf = 0, idf = 0;
			StringBuilder sb = new StringBuilder();
			for (String s : splits[1].split(",")) {
				String[] ingNum = s.split(":");
				int num = Integer.parseInt(ingNum[1]);
				if (max != 0)
					tf = num / (double) max;
				Collection<String> values = index.get(ingNum[0]);
				if (values.size() != 0)
					idf = (Math.log10(numberOfUsers / (double) values.size()) / Math
							.log10(2));
				sb.append(ingNum[0] + ":" + (tf * idf) + ",");
			}
			output.collect(new Text(splits[0]), new Text(sb.toString()));
		}

	}

	public static class ComputeSimilarityReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text arg0, Iterator<Text> arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public static class ComputeDocLengthsMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			String[] ingredients = splits[1].split(",");
			double weight = 0, sum = 0;
			for (String ingredient : ingredients) {
				double weightIng = Double.parseDouble(ingredient.split(":")[1]);
				sum += (weightIng * weightIng);
			}
			weight = Math.sqrt(sum);
			output.collect(new Text(splits[0]), new Text(weight + ""));
		}

	}

	public static class ComputeMaxFrequencyMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			int max = Integer.MIN_VALUE;
			for (String s : splits[1].split(",")) {
				String[] ingNum = s.split(":");
				int num = Integer.parseInt(ingNum[1]);
				if (num > max) {
					max = num;
				}
			}
			if (max == Integer.MIN_VALUE) {
				max = 0;
			}
			if (!splits[0].equalsIgnoreCase("null"))
				output.collect(new Text(splits[0]), new Text(max + ""));
		}

	}

	public static class ComputeSimilarityMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static Multimap<String, String> index = ArrayListMultimap
				.create();
		public static HashMap<String, Integer> maxFreq = new HashMap<String, Integer>();
		public static HashMap<String, Double> docLength = new HashMap<String, Double>();
		private static int numberOfUsers = 247859;

		@Override
		public void configure(JobConf job) {
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(fs.open(new Path(job
								.get("indexPath") + "/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					String key = splits[0].split(":")[0];
					String[] ingredients = splits[1].split(",");
					for (String ingredient : ingredients) {
						index.put(key, ingredient);
					}
				}
				br.close();
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						job.get("maxPath") + "/part-00000"))));
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					maxFreq.put(splits[0], Integer.parseInt(splits[1]));
				}
				br.close();
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(
						job.get("docPath") + "/part-00000"))));
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					docLength.put(splits[0], Double.parseDouble(splits[1]));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			HashMap<String, Double> sumWeight = new HashMap<String, Double>();
			String[] splits = text.split("\\t");
			for (String ingredient : splits[1].split(",")) {
				String[] ingNum = ingredient.split(":");
				Collection<String> users = index.get(ingNum[0]);
				for (String user : users) {
					String[] userNum = user.split(":");
					int max = maxFreq.get(userNum[0]);
					double tf = 0, idf = 0, weight = 0, sum = 0;
					int userFreq = Integer.parseInt(userNum[0]);
					if (max != 0)
						tf = userFreq / (double) max;
					idf = Math.log10(numberOfUsers / (double) users.size())
							/ Math.log10(2);
					weight = tf * idf;
					sum = weight * Double.parseDouble(ingNum[1]);
					if (!sumWeight.containsKey(userNum[0])) {
						sumWeight.put(userNum[0], sum);
					} else {
						double val = sumWeight.get(userNum[0]) + sum;
						sumWeight.put(userNum[0], val);
					}
				}
			}
			TreeSet<String> queue = new TreeSet<String>(new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					double d1 = Double.parseDouble(s1.split(":")[1]);
					double d2 = Double.parseDouble(s2.split(":")[1]);
					return d1 > d2 ? 1 : d1 == d2 ? 0 : -1;
				}
			});

			Iterator<String> userIter = sumWeight.keySet().iterator();
			while (userIter.hasNext()) {
				String user = userIter.next();
				double doclen1 = docLength.get(splits[0]), doclen2 = docLength
						.get(user), sumTotal = 0;
				sumTotal = sumWeight.get(user) / (doclen1 * doclen2);
				if (queue.size() < 10) {
					queue.add(user + ":" + sumTotal);
				} else {
					String last = queue.last();
					queue.remove(last);
					queue.add(user + ":" + sumTotal);
				}
			}
			System.out.println("My queue size is" + queue.size());
			Iterator<String> it = queue.iterator();
			StringBuilder sb = new StringBuilder();
			while (it.hasNext()) {
				String val = it.next();
				sb.append(val + ",");
			}
			output.collect(new Text(splits[0]), new Text(sb.toString()));
		}

	}

	public static class ComputeSimilarityMap2 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static HashMap<String, Double> docLength = new HashMap<String, Double>();

		@Override
		public void configure(JobConf job) {
			try {
				FileSystem fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(job.get("docPath") + "/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					docLength.put(splits[0], Double.parseDouble(splits[1]));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			if (Integer.parseInt(splits[0].split(":")[1]) > 1
					&& Integer.parseInt(splits[0].split(":")[1]) < 100000) {
				String[] ingredients = splits[1].split(",");
				HashMap<String, Double> orderedPairs = new HashMap<String, Double>();
				HashMap<String, Double> weights = new HashMap<String, Double>();
				for (String ingredient : ingredients) {
					String[] ingNum = ingredient.split(":");
					weights.put(ingNum[0], Double.parseDouble(ingNum[1]));
				}
				for (String ingredient : ingredients) {
					String[] ingNum = ingredient.split(":");
					for (int i = 0; i < ingredients.length; i++) {
						String ing = ingredients[i];
						if (ing.equals(ingredient))
							continue;
						else {
							String[] ingNum1 = ing.split(":");
							String samePair = ingNum[0] + "," + ingNum1[0];
							String reversePair = ingNum1[0] + "," + ingNum[0];
							if (orderedPairs.containsKey(samePair)
									|| orderedPairs.containsKey(reversePair))
								continue;
							else {
								orderedPairs.put(samePair, (weights
										.get(ingNum[0]) * weights
										.get(ingNum1[0])));
							}

						}
					}
					Iterator<String> it = orderedPairs.keySet().iterator();
					while (it.hasNext()) {
						String key1 = it.next();
						String[] splits1 = key1.split(",");
						String val1 = splits1[0] + ":"
								+ docLength.get(splits1[0]);
						String val2 = splits1[1] + ":"
								+ docLength.get(splits1[1]);
						String updatedKey = val1 + "," + val2;
						output.collect(new Text(updatedKey), new Text(
								orderedPairs.get(key1) + ""));
					}
				}
			}
		}

	}

	public static class ComputeSimilarityReduce2 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String[] splits = key.toString().split(",");
			String doc1 = splits[0].split(":")[0], doc2 = splits[1].split(":")[0];
			double val1 = Double.parseDouble(splits[0].split(":")[1]), val2 = Double
					.parseDouble(splits[1].split(":")[1]);
			double sum = 0;
			while (values.hasNext()) {
				sum += Double.parseDouble(values.next().toString());
			}
			double weight = sum / (val1 * val2);
			output.collect(new Text(doc1 + "," + doc2), new Text(weight + ""));
		}
	}

	public static class ComputeSimilarityMap3 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private FileSystem fs;
		private String path;

		@Override
		public void configure(JobConf job) {
			try {
				fs = FileSystem.get(job);
				path = job.get("output");
			} catch (IOException e) {

			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String[] splits = value.toString().trim().split("\\t");
			String source = splits[0].split(":")[0];
			HashMap<String, Double> pairs = new HashMap<String, Double>();
			for (String ingredient : splits[1].split(",")) {
				String[] ingNum = ingredient.split(":");
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(path + "/" + ingNum[0]))));
				String st = br.readLine().trim();
				br.close();
				String[] splits1 = st.split("\\t");
				if (Integer.parseInt(splits1[0].split(":")[1]) <= 1
						&& Integer.parseInt(splits1[0].split(":")[1]) >= 100000)
					continue;
				double userValue = 0;
				for (String userVal : splits1[1].split(",")) {
					String user = userVal.split(":")[0];
					if (user.equals(source)) {
						userValue = Double.parseDouble(userVal.split(":")[1]);
						break;
					}
				}
				for (String userVal : splits1[1].split(",")) {
					String user = userVal.split(":")[0];
					if (!user.equals(source)) {
						double prodWeights = userValue
								* Double.parseDouble(userVal.split(":")[1]);
						if (pairs.containsKey(user)) {
							double sum = pairs.get(user);
							sum += prodWeights;
							pairs.put(user, sum);
						} else {
							pairs.put(user, prodWeights);
						}
					}
				}
			}

			Iterator<String> keys = pairs.keySet().iterator();
			StringBuilder sb = new StringBuilder();
			while (keys.hasNext()) {
				String pair = keys.next();
				sb.append(pair + ":" + pairs.get(pair) + ",");
			}
			output.collect(new Text(source), new Text(sb.toString()));
		}

	}

	public static class ComputeSimilarityReduce3 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		public static HashMap<String, Double> docLength = new HashMap<String, Double>();

		@Override
		public void configure(JobConf job) {
			try {
				FileSystem fs = FileSystem.get(job);
				String st = "";
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(job.get("docPath") + "/part-00000"))));
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					docLength.put(splits[0], Double.parseDouble(splits[1]));
				}
				br.close();
			} catch (IOException e) {

			}
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			TreeSet<String> queue = new TreeSet<String>(new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					double d1 = Double.parseDouble(s1.split(":")[1]);
					double d2 = Double.parseDouble(s2.split(":")[1]);
					return d1 > d2 ? 1 : d1 == d2 ? 0 : -1;
				}
			});
			if (values.hasNext()) {
				String value = values.next().toString();
				String[] splits = value.split(",");
				for (String ing : splits) {
					double sum = docLength.get(key.toString())
							* docLength.get(ing.split(":")[0]);
					if (queue.size() < 10) {
						queue.add(ing.split(":")[0] + ":" + sum);
					} else {
						String last = queue.last();
						queue.remove(last);
						queue.add(ing.split(":")[0] + ":" + sum);
					}
				}
			}
			StringBuilder sb = new StringBuilder();
			while (!queue.isEmpty()) {
				sb.append(queue.pollFirst() + ",");
			}
			output.collect(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new UserUserSimilarityBasedOnIngredient(), args);
		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	public int run(String[] args) throws Exception {
		//createIngredientCountJob(args);
		// createIngredientId(args);
		//createReviewTextId(args);
		transformInput(args);
		// createInvertedIndexJob(args, 1, 2, false);
		// computeMaxFrequencyJob(args);
		// computeWeightsJob(args);
		// computeDocLengthsJob(args);
		// createInvertedIndexJob(args, 3, 6, false);
		// computeSimilarityJob2(args);
		// computeSimilarityJob3(args);
		// computeSimilarityJob(args);
		return 0;
	}

	private void createReviewTextId(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Create Review Text Ids");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ReviewTextIdMap.class);
		job.setReducerClass(IngredientIdReduce.class);
		// job.setNumMapTasks(1);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[10]);
		FileSystem.get(job).delete(new Path(args[11]));
		FileOutputFormat.setOutputPath(job, new Path(args[11]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	private void transformInput(String[] args) throws IOException {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Transform input");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(TransformMahoutInputMap.class);
		job.setReducerClass(IdentityReducer.class);
		// job.setNumMapTasks(1);
		job.set("ingredient", args[8]);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[1]);
		FileSystem.get(job).delete(new Path(args[9]));
		FileOutputFormat.setOutputPath(job, new Path(args[9]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	private void createIngredientId(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Create Ingredient Id's");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setLong("mapred.task.timeout", 1000 * 600 * 600);
		job.setMapperClass(IngredientIdMap.class);
		job.setReducerClass(IngredientIdReduce.class);
		// job.setNumMapTasks(1);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[10]);
		FileSystem.get(job).delete(new Path(args[8]));
		FileOutputFormat.setOutputPath(job, new Path(args[8]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	@SuppressWarnings("deprecation")
	private void computeSimilarityJob3(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.set("docPath", args[4]);
		job.set("output", args[6]);
		job.setJobName("Compute similar neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setLong("mapred.task.timeout", 1000 * 600 * 600);
		job.setMapperClass(ComputeSimilarityMap3.class);
		job.setReducerClass(ComputeSimilarityReduce3.class);
		job.setNumMapTasks(120);
		job.setNumReduceTasks(120);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[1]);
		FileSystem.get(job).delete(new Path(args[7]));
		FileOutputFormat.setOutputPath(job, new Path(args[7]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	@SuppressWarnings({ "deprecation", "unused" })
	private void computeSimilarityJob2(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.set("docPath", args[4]);
		job.setJobName("Compute similar neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ComputeSimilarityMap2.class);
		job.setReducerClass(ComputeSimilarityReduce2.class);
		job.setNumMapTasks(120);
		job.setNumReduceTasks(120);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[6]);
		FileSystem.get(job).delete(new Path(args[7]));
		FileOutputFormat.setOutputPath(job, new Path(args[7]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	@SuppressWarnings({ "deprecation", "unused" })
	private void computeSimilarityJob(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.set("indexPath", args[2]);
		job.set("docPath", args[4]);
		job.set("maxPath", args[5]);
		job.setJobName("Compute similar neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ComputeSimilarityMap.class);
		job.setReducerClass(IdentityReducer.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[3]);
		FileSystem.get(job).delete(new Path(args[6]));
		FileOutputFormat.setOutputPath(job, new Path(args[6]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
	}

	@SuppressWarnings("deprecation")
	private void computeMaxFrequencyJob(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Compute maximum frequency of each user");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ComputeMaxFrequencyMap.class);
		job.setReducerClass(IdentityReducer.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[1]);
		FileSystem.get(job).delete(new Path(args[5]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	@SuppressWarnings("deprecation")
	private void computeDocLengthsJob(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Compute document lengths");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ComputeDocLengthsMap.class);
		job.setReducerClass(IdentityReducer.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[3]);
		FileSystem.get(job).delete(new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
	}

	@SuppressWarnings("deprecation")
	private void computeWeightsJob(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Compute Weights");
		job.set("indexPath", args[2]);
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ComputeWeightsMap.class);
		job.setReducerClass(IdentityReducer.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[1]);
		FileSystem.get(job).delete(new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

	@SuppressWarnings("deprecation")
	private void createInvertedIndexJob(String[] args, int input, int output,
			Boolean b) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Inverted Index");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(InvertedIndexMap.class);
		job.setReducerClass(InvertedIndexReduce.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.set("output", args[output]);
		job.set("createFiles", b.toString());
		if (b == true) {
			FileInputFormat.setInputPaths(job, args[input]);
			FileSystem.get(job).delete(new Path(args[output]));
			FileSystem.get(job).delete(new Path("/tmp/DeleteThisDirectory1"));
			FileOutputFormat.setOutputPath(job, new Path(
					"/tmp/DeleteThisDirectory1"));
			JobClient.runJob(job);
		} else {
			FileInputFormat.setInputPaths(job, args[input]);
			FileSystem.get(job).delete(new Path(args[output]));
			FileOutputFormat.setOutputPath(job, new Path(args[output]));
			JobClient.runJob(job);
		}
		System.out.println("***********DONE********");

	}

	@SuppressWarnings("deprecation")
	private void createIngredientCountJob(String[] args) throws IOException {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Ingredient Counts");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UserUserSimilarityBasedOnIngredientMap.class);
		job.setReducerClass(UserUserSimilarityBasedOnIngredientReduce.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");

	}

}
