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

public class CreateInputForCosineSimilarityForHeterogeneous extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class CreateInputForCosineSimilarityForHeterogeneousMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			String[] keys = text.split("\\t");
			try {
				String[] splits = keys[1].split(",");
				output.collect(new Text(keys[0]), new Text(splits[0]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class CreateInputForCosineSimilarityForHeterogeneousReduce extends MapReduceBase
			implements Reducer<Text,Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			StringBuffer buffer = new StringBuffer();
			while(values.hasNext()){
				buffer.append(values.next()+",");
			}
			output.collect(key, new Text(buffer.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new CreateInputForCosineSimilarityForHeterogeneous(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Create cosine similarity input");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(CreateInputForCosineSimilarityForHeterogeneousMap.class);
		job.setReducerClass(CreateInputForCosineSimilarityForHeterogeneousReduce.class);
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
