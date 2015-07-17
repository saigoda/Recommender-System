package CreateGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

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

public class GenerateTrainAndTestFolds extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class GenerateTrainAndTestFoldsMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
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

	public static class GenerateTrainAndTestFoldsReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			int count = 0;
			List<String> list = new ArrayList<String>();
			while (values.hasNext()) {
				list.add(values.next().toString());
				count++;
			}
			if (count != 1) {
				long testCount = Math.round(((double) count) * 0.7);
				int i = 1;
				Random r = new Random();
				HashSet<Integer> takenIndexes = new HashSet<Integer>();
				while (i <= testCount) {
					int index = r.nextInt(count - 1);
					if (takenIndexes.contains(index))
						continue;
					else {
						takenIndexes.add(index);
						i++;
						output.collect(new Text("0"), new Text(key.toString()
								+ "," + list.get(index)));
					}
				}
				for (int j = 0; j < count; j++) {
					if (takenIndexes.contains(j))
						continue;
					output.collect(new Text("1"), new Text(key.toString() + ","
							+ list.get(j)));
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new GenerateTrainAndTestFolds(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Generate Train and Test Fold Step 1");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateTrainAndTestFoldsMap.class);
		job.setReducerClass(GenerateTrainAndTestFoldsReduce.class);
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
