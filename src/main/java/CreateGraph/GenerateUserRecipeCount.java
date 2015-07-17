package CreateGraph;

import java.io.IOException;
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
import org.crawling.models.Cook;

import com.google.gson.Gson;

public class GenerateUserRecipeCount extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class GenerateUserRecipeCountMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				Cook cook = new Gson().fromJson(text, Cook.class);
				List<String> recipes = cook.getNamesOfRecipesInRecipeBox();
				for (int i = 0; i < recipes.size(); i++) {
					if (cook.getTypesOfRecipesInRecipeBox().get(i)
							.toLowerCase().equals("kitchen approved") && recipes.get(i).contains("_recipe")) {
						String recipe = recipes.get(i);
						String outputKey = cook.getName() + "," + recipe
								+ ",";
						if (cook.getRatingGivenByUserToRecipe().get(i) == 0) {
							output.collect(new Text(outputKey + "1"),
									new Text(""));
						} else {
							output.collect(new Text(outputKey
									+ cook.getRatingGivenByUserToRecipe()
											.get(i)), new Text(""));
						}
					}
				}
				if (cook.getPostedRecipe() != null) {
					for (String recipe : cook.getPostedRecipe()) {
						String outputKey = "" + cook.getName() + "," + recipe
								+ ",";
						output.collect(new Text(outputKey + "5"), new Text(""));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class GenerateUserRecipeCountReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			output.collect(key, new Text(""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new GenerateUserRecipeCount(),
					args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate User Recipe Count");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateUserRecipeCountMap.class);
		job.setReducerClass(GenerateUserRecipeCountReduce.class);
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
