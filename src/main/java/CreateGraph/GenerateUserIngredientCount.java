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
import org.apache.hadoop.io.IntWritable;
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
import org.crawling.models.Cook;
import org.crawling.models.ReviewDish;

import com.google.gson.Gson;

public class GenerateUserIngredientCount extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class UpdateUserInformationMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, IntWritable> {

		public static HashMap<String, ReviewDish> recipesMap = new HashMap<String, ReviewDish>();

		static {
			try {
				Path cookIdFilePath = new Path(
						"/user/saibharath/UniqueRecipeJsonFinal/part-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(cookIdFilePath)));
				String str = "";
				while ((str = br.readLine()) != null) {
					ReviewDish dish = new Gson()
							.fromJson(str, ReviewDish.class);
					recipesMap.put(dish.getName(), dish);
				}
				br.close();

			} catch (Exception e) {

			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				Cook cook = new Gson().fromJson(text, Cook.class);
				List<String> recipes = cook.getNamesOfRecipesInRecipeBox();
				for (int i = 0; i < recipes.size(); i++) {
					if (cook.getTypesOfRecipesInRecipeBox().get(i)
							.toLowerCase().equals("kitchen approved")
							&& cook.getRatingGivenByUserToRecipe().get(i) >= 4) {
						String recipe = recipes.get(i);
						ReviewDish dish = recipesMap.get(recipe);
						List<String> ingredients = dish.getIngredientsName();
						for (String ingredient : ingredients) {
							String outputKey = "(" + cook.getName() + ","
									+ ingredient + ")";
							output.collect(new Text(outputKey),
									new IntWritable(1));
						}
					}
				}
				if (cook.getPostedRecipe() != null) {
					for (String recipe : cook.getPostedRecipe()) {
						for (String ingredient : recipesMap.get(recipe)
								.getIngredientsName()) {
							String outputKey = "(" + cook.getName() + ","
									+ ingredient + ")";
							output.collect(new Text(outputKey),
									new IntWritable(1));
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class UpdateUserInformationReduce extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int count = 0;
			while (values.hasNext())
				count += values.next().get();
			String updatedKey = key.toString().substring(0,
					key.toString().length() - 1)
					+ "," + count + ")";
			output.collect(new Text(updatedKey), new Text(""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new GenerateUserIngredientCount(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate User Ingredient Count");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UpdateUserInformationMap.class);
		job.setReducerClass(UpdateUserInformationReduce.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
