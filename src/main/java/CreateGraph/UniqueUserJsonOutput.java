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
import org.crawling.models.Cook;

import com.google.gson.Gson;

public class UniqueUserJsonOutput extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class UniqueUserJsonOutputMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Cook> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Cook> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				Cook cook = new Gson().fromJson(text, Cook.class);
				System.out.println(cook.getName());
				if (cook.getName().length() > 1) {
					output.collect(new Text(cook.getName()), cook);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static class UniqueUserJsonOutputReduce extends MapReduceBase
			implements Reducer<Text, Cook, Text, Text> {
		public static HashMap<String, String> userId = new HashMap<String, String>();
		public static HashMap<String, String> recipeId = new HashMap<String, String>();
		static {
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
			try {
				Path recipeIdFilePath = new Path(
						"/user/saibharath/RecipeId/part-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(recipeIdFilePath)));
				String str = "";
				while ((str = br.readLine()) != null) {
					String[] splits = str.split("\\t");
					if (splits.length == 2)
						recipeId.put(splits[1].trim(), splits[0].trim());
				}
				br.close();

			} catch (Exception e) {

			}
		}

		public void reduce(Text key, Iterator<Cook> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String userIdForKey = userId.get(key.toString());
			Cook cook = values.next();
			cook.setName(userIdForKey);
			for (int i = 0; i < cook.getNamesOfRecipesInRecipeBox().size(); i++) {
				String recipeName = cook.getNamesOfRecipesInRecipeBox().get(i);
				String type = cook.getTypesOfRecipesInRecipeBox().get(i);
				type = type.toLowerCase();
				if(type.equals("kitchen approved")){
					if(recipeId.containsKey(recipeName))
						cook.getNamesOfRecipesInRecipeBox().set(i, recipeId.get(recipeName));
					else
						System.out.println("This is a kitchen approved recipe but does not have an entry");
				}
			}

			output.collect(new Text(new Gson().toJson(cook)), new Text(""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new UniqueUserJsonOutput(),
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
		job.setJobName("UniqueUserJsonFile");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UniqueUserJsonOutputMap.class);
		job.setReducerClass(UniqueUserJsonOutputReduce.class);
		job.setNumMapTasks(1);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Cook.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
