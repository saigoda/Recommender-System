package CreateGraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

public class CountRecipesInTestAndTrain extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class CountRecipesInTestAndTrainMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split("\\t");
				StringTokenizer st = new StringTokenizer(splits[1], ",");
				while(st.hasMoreTokens())
					output.collect(new Text(st.nextToken()), new IntWritable(1));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class CountRecipesInTestAndTrainReduce extends MapReduceBase
			implements Reducer<Text,IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			int count = 0;
			while(values.hasNext()){
				count+=values.next().get();
			}
			output.collect(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new CountRecipesInTestAndTrain(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Count recipes for train and test");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(CountRecipesInTestAndTrainMap.class);
		job.setReducerClass(CountRecipesInTestAndTrainReduce.class);
		//job.setNumMapTasks(50);
		job.setNumReduceTasks(30);
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
