package CreateGraph;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class FormattedRecsMap extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class FormattedRecsMapMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split("\\t");
				for (String s : splits[1].split(","))
					output.collect(new Text(splits[0] + "_user"), new Text(s
							+ "_recipe"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class FormattedRecsMapReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			StringBuffer buffer = new StringBuffer();
			while (values.hasNext()) {
				buffer.append(values.next() + ",");
			}
			output.collect(key, new Text(buffer.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new FormattedRecsMap(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
//		JobConf job = new JobConf(super.getConf(), this.getClass());
//		job.setJarByClass(this.getClass());
//		job.setJobName("Find user recipes in train");
//		job.setJobPriority(JobPriority.VERY_HIGH);
//		job.setMapperClass(FormattedRecsMapMap.class);
//		job.setReducerClass(FormattedRecsMapReduce.class);
//		// job.setNumMapTasks(50);
//		job.setNumReduceTasks(1);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		FileInputFormat.setInputPaths(job, args[0]);
//		FileSystem.get(job).delete(new Path(args[1]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		JobClient.runJob(job);
//		System.out.println("***********DONE********");
	    SimpleDateFormat sd = new SimpleDateFormat("MM/dd/yyyy");
	    Date d = new Date("12/7/2014");
	    Date d1 = new Date("12/7/2012");
	    //sd.format(d)
	    long d4=(((sd.parse("12/6/2014").getTime()-sd.parse(sd.format(d1)).getTime())/(1000*86400))/365);
	    long d5 = d4*365;
	    long d6 = (((sd.parse("12/6/2014").getTime()-sd.parse(sd.format(d1)).getTime())/(1000*86400))-d5);
		System.out.println(d6);
		System.out.println(d.getDay());
		System.out.println(d.getYear());
		return 0;
	}

}
class CreditCard{
    
}
