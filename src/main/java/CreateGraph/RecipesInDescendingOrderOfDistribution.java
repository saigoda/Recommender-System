package CreateGraph;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

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

public class RecipesInDescendingOrderOfDistribution extends Configured
		implements Tool {

	/**
	 * @param args
	 */
	public static class RecipesInDescendingOrderOfDistributionMap extends
			MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split(",");
				String outputText = splits[1] + "," + splits[2];
				output.collect(new Text(splits[0]), new Text(outputText));
			} catch (Exception e) {

			}
		}

	}

	public static class RecipesInDescendingOrderOfDistributionReduce extends
			MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			PriorityQueue<String> queue = new PriorityQueue<String>(100000,
					new Comparator<String>() {
						@Override
						public int compare(String s1, String s2) {
							return Integer
									.valueOf(
											Integer.parseInt(s2.split(",")[1]))
									.compareTo(
											Integer.parseInt(s1.split(",")[1]));
						}
					});
			while (values.hasNext()) {
				queue.add(values.next().toString());
			}
			String outputValue = "";
			while (!queue.isEmpty()) {
				String queueVal = queue.remove();
				outputValue += (queueVal.split(",")[0] + ",");
			}
			output.collect(key, new Text(outputValue));

		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new RecipesInDescendingOrderOfDistribution(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("User Recipe Test Formatted Data");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(RecipesInDescendingOrderOfDistributionMap.class);
		job.setReducerClass(RecipesInDescendingOrderOfDistributionReduce.class);
		//job.setNumReduceTasks(30);
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
