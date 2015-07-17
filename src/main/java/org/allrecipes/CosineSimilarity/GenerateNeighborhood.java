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

public class GenerateNeighborhood extends Configured implements Tool {

	public static class GenerateNeighborhoodMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static HashMap<String, Integer> count = new HashMap<String, Integer>();

		@Override
		public void configure(JobConf job) {
			try {
				FileSystem fs = FileSystem.get(job);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path(job.get("count") + "/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					count.put(splits[0], Integer.parseInt(splits[1]));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			output.collect(new Text(splits[0] + ":" + count.get(splits[0])),
					new Text(splits[1] + ":" + count.get(splits[1]) + ":"
							+ splits[2]));
		}
	}

	public static class GenerateNeighborhoodReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {

			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append(values.next().toString()+",");
			}
			output.collect(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new GenerateNeighborhood(), args);
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
		job.set("count", args[1]);
		job.setJobName("Generate Neighborhood for 10 neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateNeighborhoodMap.class);
		job.setReducerClass(GenerateNeighborhoodReduce.class);
		// job.setNumMapTasks(20);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
