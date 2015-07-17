package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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

import com.google.gson.Gson;

public class ExtractIngredientsFromRecipes extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class ExtractIngredientsFromRecipesMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		static HashMap<String, List<String>> recipes = new HashMap<String, List<String>>();
		static {
			try {
				Path recipeIdFilePath = new Path(
						"/user/saibharath/UniqueRecipeJsonFinal/part-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(recipeIdFilePath)));
				String str = "";
				while ((str = br.readLine()) != null) {
					ReviewDish dish = new Gson()
							.fromJson(str, ReviewDish.class);
					recipes.put(dish.getName(), dish.getIngredientsName());
				}
				br.close();

			} catch (Exception e) {

			}

		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String splits[] = text.split(",");
				int rating = Integer.parseInt(splits[2]);
				if (rating >= 4) {
					List<String> ingredients = recipes.get(splits[1]);
					for (String ingredient : ingredients) {
						output.collect(new Text(splits[0]),
								new Text(ingredient));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ExtractIngredientsFromRecipesReduce extends
			MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashMap<String, Integer> ingredientCount = new HashMap<String, Integer>();
			while (values.hasNext()) {
				String ingredient = values.next().toString();
				if (ingredientCount.containsKey(ingredient)) {
					int count = ingredientCount.get(ingredient);
					count++;
					ingredientCount.put(ingredient, count);
				} else {
					ingredientCount.put(ingredient, 1);
				}
			}
			Iterator<String> keysIterator = ingredientCount.keySet().iterator();
			while (keysIterator.hasNext()) {
				String keys = keysIterator.next();
				output.collect(new Text(key.toString() + "," + keys + ","
						+ ingredientCount.get(keys)), new Text(""));
			}
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new ExtractIngredientsFromRecipes(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Extract Ingredients From Train Data");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ExtractIngredientsFromRecipesMap.class);
		job.setReducerClass(ExtractIngredientsFromRecipesReduce.class);
		// job.setNumMapTasks(50);
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
