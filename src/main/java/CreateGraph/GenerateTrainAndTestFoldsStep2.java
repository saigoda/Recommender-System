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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenerateTrainAndTestFoldsStep2 extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class GenerateTrainAndTestFoldsStep2Map extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split("\\t");
				output.collect(new Text(splits[0].trim()), new Text(splits[1].trim()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	public static class GenerateTrainAndTestFoldsStep2Partitioner implements Partitioner<Text, Text> {

		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

	   public int getPartition(Text key, Text value, int numReduceTasks) {
	        	if(key.toString().equals("0"))
	        		return 0;
	        	else
	        		return 1;
	        }		 
       
	}
	public static class GenerateTrainAndTestFoldsStep2Reduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				output.collect(new Text(values.next().toString()),new Text(""));
			}
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new GenerateTrainAndTestFoldsStep2(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate Train and Test Fold Test 2");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateTrainAndTestFoldsStep2Map.class);
		job.setReducerClass(GenerateTrainAndTestFoldsStep2Reduce.class);
		job.setPartitionerClass(GenerateTrainAndTestFoldsStep2Partitioner.class);
		job.setNumReduceTasks(2);
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
