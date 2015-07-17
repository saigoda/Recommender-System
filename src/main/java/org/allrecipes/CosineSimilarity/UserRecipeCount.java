package org.allrecipes.CosineSimilarity;

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
import org.crawling.models.Review;

import com.google.gson.Gson;

public class UserRecipeCount extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static class UserRecipeCountMap extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {
		
		private static HashMap<String, Integer> userId = new HashMap<String,Integer>();
		
		private static HashMap<String, Integer> recipeId = new HashMap<String,Integer>();

		@Override
		public void configure(JobConf job) {
			readIds("allrecipes/id/UserID/part-r-00000", userId, job);
			readIds("allrecipes/id/RecipeID/part-r-00000", recipeId, job);
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
			try {
				if (text.length() > 0) {
					Review review = new Gson().fromJson(text, Review.class);
					if (review.getRating() >= 3) {
						output.collect(new Text(userId.get(review.getReviewer().getName())+""), new Text(recipeId.get(review.getReviewDish().getName())+""));
					}
				}
			}catch(Exception e){
				
			}
		}
		
	}
	
	public static class UserRecipeCountReduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			int size = 0;
			while(values.hasNext()){
				values.next();
				size++;}
			output.collect(key, new Text(size+""));
		}
		
	}
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new UserRecipeCount(), args);
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
		job.setJobName("Compute recipe count");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UserRecipeCountMap.class);
		job.setReducerClass(UserRecipeCountReduce.class);
		//job.setNumMapTasks(20);
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
