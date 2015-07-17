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
import org.crawling.models.ReviewDish;

import com.google.gson.Gson;

public class UserPostedRecipes extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class UserPostedRecipesMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			ReviewDish dish = new Gson().fromJson(text, ReviewDish.class);
			if (dish.getPostedBy() != null)
				if (dish.getPostedBy().getName() != null)
					if (dish.getPostedBy().getName().length() > 0)
						output.collect(new Text(dish.getPostedBy().getName()),
								new Text(dish.getName()));
		}

	}

	public static class UserPostedRecipesReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String val = "";
			while (values.hasNext())
				val += values.next() + ",";
			val = val.substring(0, val.length() - 1);
			output.collect(key, new Text(val));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new UserPostedRecipes(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Finding User posted Recipes");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UserPostedRecipesMap.class);
		job.setReducerClass(UserPostedRecipesReduce.class);
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
