package CreateGraph;

import java.io.IOException;
import java.util.ArrayList;
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

public class GenerateDistributionForInput extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class GenerateDistributionMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				text = text.trim();
				text = text.replace("(", "");
				text = text.replace(")", "");
				String[] splits = text.split(",");
				int count = Integer.parseInt(splits[2]);
				String source = splits[0];
				String dest = splits[1] + "," + count;
				output.collect(new Text(source), new Text(dest));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class GenerateDistributionReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			int totalCount = 0;
			List<String> destinationCounts = new ArrayList<String>();
			while (values.hasNext()) {
				String val = values.next().toString();
				String[] splits = val.split(",");
				totalCount += Integer.parseInt(splits[1]);
				destinationCounts.add(val);
			}
			for (int i = 0; i < destinationCounts.size(); i++) {
				String val = destinationCounts.get(i);
				String[] splits = val.split(",");
				double dist = Integer.parseInt(splits[1]) / (double) totalCount;
				String outputKey = key.toString() + "," + splits[0] + ","
						+ dist;
				output.collect(new Text(outputKey), new Text(""));
			}
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new GenerateDistributionForInput(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate Distribution for an input");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateDistributionMap.class);
		job.setReducerClass(GenerateDistributionReduce.class);
		job.setNumMapTasks(50);
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
