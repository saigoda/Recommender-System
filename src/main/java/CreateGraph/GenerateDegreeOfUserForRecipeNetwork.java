package CreateGraph;

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

public class GenerateDegreeOfUserForRecipeNetwork extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class GenerateDegreeOfUserForRecipeNetworkMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split(",");
				output.collect(new Text(splits[0]), new Text(splits[1]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class GenerateDegreeOfUserForRecipeNetworkReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			int count = 0;
			StringBuilder sb = new StringBuilder();
			while(values.hasNext()){
				sb.append(values.next().toString()+",");
				count++;
			}
			output.collect(new Text(key.toString()+":"+count), new Text(sb.substring(0, sb.length()-1)));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new GenerateDegreeOfUserForRecipeNetwork(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate Degree Of User For Input Network");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateDegreeOfUserForRecipeNetworkMap.class);
		job.setReducerClass(GenerateDegreeOfUserForRecipeNetworkReduce.class);
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
