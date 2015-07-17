package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

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

public class UniqueRecipeJsonOutput extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class UniqueRecipeJsonOutputMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, ReviewDish> {
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, ReviewDish> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				ReviewDish recipe = new Gson().fromJson(text, ReviewDish.class);
				if (recipe.getName().length() > 0) {
					output.collect(new Text(recipe.getName()),recipe);
				}
			} catch (Exception e) {

			}
		}

	}

	public static class UniqueRecipeJsonOutputReduce extends MapReduceBase
			implements Reducer<Text, ReviewDish, Text, Text> {
		public static HashMap<String,String> recipeId = new HashMap<String,String>();
		static
		{
			try
			{
				Path recipeIdFilePath = new Path("/user/saibharath/RecipeId/part-00000");
				FileSystem fs =FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(recipeIdFilePath)));
				String str = "";
				while((str = br.readLine()) != null){
					String[] splits = str.split("\\t");
					if(splits.length == 2)
					recipeId.put(splits[1].trim(),splits[0].trim());
				}
				br.close();
				
			}catch(Exception e){
				
			}
		}
		
		public void reduce(Text key, Iterator<ReviewDish> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String recipeIdForKey = recipeId.get(key.toString());
			ReviewDish dish = values.next();
			dish.setName(recipeIdForKey);
			output.collect(new Text(new Gson().toJson(dish)), new Text(""));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new UniqueRecipeJsonOutput(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("UniqueRecipeJsonFile");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UniqueRecipeJsonOutputMap.class);
		job.setReducerClass(UniqueRecipeJsonOutputReduce.class);
		job.setNumMapTasks(1);
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
