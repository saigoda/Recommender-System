package CreateGraph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

public class CreateNetworkLinkFiles extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class CreateNetworkLinkFilesMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {
		
		
		/*
		 *
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 * value -- user:count \t item1,item2....
		 * output: key--item1  value--user1:count,user2:count,user3:count...
		 * 		   key--item2  value-user1:count,user2:count..
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split("\\t");
				String user_count = splits[0];
				String[] items = splits[1].split(",");
				if (items.length == 0)
					output.collect(new Text(splits[1]), new Text(user_count));
				else {
					for (String item : items)
						output.collect(new Text(item), new Text(user_count));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class CreateNetworkLinkFilesReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		
		private static String outputPath;

		@Override
		public void configure(JobConf job) {
			outputPath = job.get("output");
		}

		private void createFile(String fileName, String content) {
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				Path path = new Path(outputPath + "/" + fileName);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						fs.create(path, true)));
				bw.write(fileName+"\t"+content);
				bw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		/*
		 * 
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 * key - Item
		 * value -- user:count,user:count..
		 * Output --Files with item name are created and they store the user information 
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append(values.next().toString() + ",");
			}
			createFile(key.toString(), sb.substring(0, sb.length() - 1));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new CreateNetworkLinkFiles(),
					args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Create Files of Each Network Type");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(CreateNetworkLinkFilesMap.class);
		job.setReducerClass(CreateNetworkLinkFilesReduce.class);
		job.set("output", args[1]);
		job.setNumMapTasks(50);
		job.setNumReduceTasks(30);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/DeleteThisDirectory1"));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
