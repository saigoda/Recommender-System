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

public class CreateUserIngredientTestFold extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class CreateUserIngredientTestFoldMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			String[] splits = text.split(",");
			output.collect(new Text(splits[0]), new Text(splits[1] + ","
					+ splits[2]));
		}
	}

	public static class CreateUserIngredientTestFoldReduce extends
			MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer buffer = new StringBuffer();
			PriorityQueue<String> queue = new PriorityQueue<String>(10000000,
					new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							int s1 = Integer.parseInt(o1.split(",")[1]);
							int s2 = Integer.parseInt(o2.split(",")[1]);
							return s2 > s1 ? 1 : s2 == s1 ? 0 : -1;
						}

					});
			while (values.hasNext()) {
				queue.add(values.next().toString());
			}
			while (!queue.isEmpty()) {
				buffer.append(queue.remove().split(",")[0] + ",");
			}
			output.collect(key, new Text(buffer.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new CreateUserIngredientTestFold(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("User Ingredient Test Fold");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(CreateUserIngredientTestFoldMap.class);
		job.setReducerClass(CreateUserIngredientTestFoldReduce.class);
		// job.setNumMapTasks(50);
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
