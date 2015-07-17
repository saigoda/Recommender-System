package CreateGraph;

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
import org.crawling.models.ReviewDish;
import org.crawling.models.Reviewer;
import org.tartarus.snowball.ext.EnglishStemmer;

import com.google.gson.Gson;

public class UniqueRecipeJsonOutputFinal extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class UniqueRecipeJsonOutputFinalMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, ReviewDish> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, ReviewDish> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();

			try {
				ReviewDish recipe = new Gson().fromJson(text, ReviewDish.class);
				output.collect(new Text(recipe.getName()), recipe);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class UniqueRecipeJsonOutputFinalReduce extends MapReduceBase
			implements Reducer< Text,ReviewDish, Text, Text> {
		public static HashMap<String, String> ingredientId = new HashMap<String, String>();
		public static HashSet<String> removeWords = new HashSet<String>();
		public static HashMap<String, String> userId = new HashMap<String, String>();
		static {
			try {
				Path recipeIdFilePath = new Path(
						"/user/saibharath/IngredientId/part-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(recipeIdFilePath)));
				String str = "";
				while ((str = br.readLine()) != null) {
					String[] splits = str.split("\\t");
					if (splits.length == 2)
						ingredientId.put(splits[1].trim(), splits[0].trim());
				}
				br.close();

			} catch (Exception e) {

			}
			try {
				Path path = new Path("/user/saibharath/Remove/RemoveWords.txt");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(path)));
				String str = "";
				while ((str = br.readLine()) != null) {
					if (str.trim().length() > 0) {
						removeWords.add(str.toLowerCase().trim());
					}
				}
			} catch (Exception e) {

			}
			try {
				Path cookIdFilePath = new Path(
						"/user/saibharath/UserId/part-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(cookIdFilePath)));
				String str = "";
				while ((str = br.readLine()) != null) {
					String[] splits = str.split("\\t");
					if (splits.length == 2)
						userId.put(splits[1].trim(), splits[0].trim());
				}
				br.close();

			} catch (Exception e) {

			}
		}

		public static String stemAndTrim(String input) {
			String newSplits = "";
			for (String newSplit : input.split("\\s")) {
				if (!removeWords.contains(newSplit)) {
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

		public void reduce(Text key1, Iterator<ReviewDish> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			ReviewDish key = values.next();
			List<String> ingredientNames = new ArrayList<String>();
			for(String ingredient : key.getIngredientsName()) {
				ingredient = ingredient.replaceAll("[^a-zA-Z\\s]", "");
				ingredient = ingredient.replaceAll("\\t", "");
				ingredient = ingredient.trim();
				ingredient = ingredient.toLowerCase();
				if (ingredient.contains(" or")) {
					List<String> ingredientSplits = new ArrayList<String>();
					ingredientSplits.add(ingredient.substring(0,
							ingredient.indexOf(" or")));
					ingredientSplits
							.add(ingredient.substring(
									ingredient.indexOf(" or") + 3,
									ingredient.length()));
					for (String splits : ingredientSplits) {
						splits = stemAndTrim(splits);
						if (splits.length() > 1) {
							ingredientNames.add(ingredientId.get(splits));
						}

					}
				} else {
					ingredient = stemAndTrim(ingredient);
					if (ingredient.length() > 1) {
						ingredientNames.add(ingredientId.get(ingredient));
					}
				}
			}
			key.setIngredientsName(ingredientNames);
			if (key.getPostedBy().getName().length() > 0) {
				Reviewer postedBy = new Reviewer();
				postedBy.setName(userId.get(key.getPostedBy().getName()));
				key.setPostedBy(postedBy);
			}
			output.collect(new Text(new Gson().toJson(key, ReviewDish.class)),
					new Text(""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new UniqueRecipeJsonOutputFinal(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Unique Recipe Json File with user Id");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UniqueRecipeJsonOutputFinalMap.class);
		job.setReducerClass(UniqueRecipeJsonOutputFinalReduce.class);
		job.setNumMapTasks(10);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ReviewDish.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
