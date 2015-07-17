package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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

public class UpdateUserInformation extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class UpdateUserInformationMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Cook> {
		

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Cook> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				Cook cook = new Gson().fromJson(text, Cook.class);
				if (cook.getName().length() > 1) {
					output.collect(new Text(cook.getName()), cook);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static class UpdateUserInformationReduce extends MapReduceBase
			implements Reducer<Text, Cook, Text, Text> {
		public static HashMap<String, List<String>> userPostedRecipes = new HashMap<String, List<String>>();

		static {
			try {
				Path cookIdFilePath = new Path(
						"/user/saibharath/UserPostedRecipes/part-00000");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(cookIdFilePath)));
				String str = "";
				while ((str = br.readLine()) != null) {
					String[] splits = str.split("\\t");
					if (splits.length == 2) {
						List<String> recipes = new ArrayList<String>();
						if (splits[1].contains(",")) {
							StringTokenizer st = new StringTokenizer(splits[1],
									",");
							while (st.hasMoreTokens()) {
								recipes.add(st.nextToken());
							}
						} else {
							recipes.add(splits[1]);
						}
						userPostedRecipes.put(splits[0], recipes);
					}
				}
				br.close();

			} catch (Exception e) {

			}
		}
		public void reduce(Text key, Iterator<Cook> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
		
			Cook cook = values.next();
			if(userPostedRecipes.containsKey(key.toString()))
				cook.setPostedRecipe(userPostedRecipes.get(cook.getName()));
			output.collect(new Text(new Gson().toJson(cook)), new Text(""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new UpdateUserInformation(),
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
		job.setJobName("Updated User Information Containing Posted Recipes");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UpdateUserInformationMap.class);
		job.setReducerClass(UpdateUserInformationReduce.class);
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
