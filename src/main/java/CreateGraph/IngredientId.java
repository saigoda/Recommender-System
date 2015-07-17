package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
import org.tartarus.snowball.ext.EnglishStemmer;

import com.google.gson.Gson;

public class IngredientId extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class IngredientIdMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static HashSet<String> removeWords = new HashSet<String>();
		static {
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
		}

		public static String stemAndTrim(String input) {
			String newSplits = "";
			for (String newSplit : input.split("\\s")) {
				if (!removeWords.contains(newSplit)) {
					EnglishStemmer stemmer = new EnglishStemmer();
					stemmer.setCurrent(newSplit);
					stemmer.stem();
					newSplit = stemmer.getCurrent();
					if(newSplit.length() > 1)
					newSplits += newSplit + " ";
				}
			}
			return newSplits.trim();
			
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				ReviewDish recipe = new Gson().fromJson(text, ReviewDish.class);
				for (String ingredient : recipe.getIngredientsName()) {
					ingredient = ingredient.replaceAll("[^a-zA-Z\\s]", "");
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
							if (splits.length() > 1)
								output.collect(new Text(splits), new Text());
						}
					} else {
						ingredient = stemAndTrim(ingredient);
						if (ingredient.length() > 1)
							output.collect(new Text(ingredient), new Text(""));
					}
				}

			} catch (Exception e) {

			}
		}

	}

	public static class IngredientIdReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public static enum Ingredient_Counter {
			IngredientCounter;
		};

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			if (reporter.getCounter(Ingredient_Counter.IngredientCounter)
					.getValue() == 0)
				reporter.getCounter(Ingredient_Counter.IngredientCounter)
						.increment(1);
			output.collect(
					new Text(reporter.getCounter(
							Ingredient_Counter.IngredientCounter).getValue()
							+ "_ingredient"), new Text(key));
			reporter.getCounter(Ingredient_Counter.IngredientCounter)
					.increment(1);
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new IngredientId(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("IngredientId");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(IngredientIdMap.class);
		job.setReducerClass(IngredientIdReduce.class);
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

}
