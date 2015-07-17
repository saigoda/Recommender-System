
package org.dblp.Id;

import java.io.IOException;
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

public class GenerateId extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class GenerateIdMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split("\\t");
				output.collect(new Text(splits[0]), new Text(""));
			} catch (Exception e) {

			}
		}

	}

	public static class GenerateIdReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		public static enum Recipe_Counter {
			RecipeCounter(1);
			private Recipe_Counter(int number) {
				this.number = number;
			}

			public int getNumber() {
				return number;
			}

			private int number;
		};

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			if (reporter.getCounter(Recipe_Counter.RecipeCounter).getValue() == 0)
				reporter.getCounter(Recipe_Counter.RecipeCounter).increment(1);
			output.collect(
					new Text(reporter.getCounter(Recipe_Counter.RecipeCounter)
							.getValue()+""), key);
			reporter.getCounter(Recipe_Counter.RecipeCounter).increment(1);
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new GenerateId(),
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
		job.setJobName("Generate Id for articles");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateIdMap.class);
		job.setReducerClass(GenerateIdReduce.class);
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
