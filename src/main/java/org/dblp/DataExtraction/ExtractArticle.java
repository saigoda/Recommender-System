package org.dblp.DataExtraction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtractArticle extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static class ExtractArticleMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private String type;
		public static HashMap<String, Integer> ids = new HashMap<String, Integer>();

		@Override
		public void configure(JobConf conf) {
			type = conf.get("type");
			try {
				FileSystem fs = FileSystem.get(conf);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(new Path("dblp/id/part-00000"))));
				String st = "";
				while ((st = br.readLine()) != null) {
					String[] splits = st.split("\\t");
					ids.put(splits[1], Integer.parseInt(splits[0]));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString();
			try {
				String[] splits = text.split("\\t");

				String[] attributes = splits[1].split("\\|");
				int noOfAuthors = Integer.parseInt(attributes[3]);
				int noOfCitations = Integer
						.parseInt(attributes[3 + noOfAuthors + 1]);
				if (noOfCitations == 0) {
					if (attributes[0].equalsIgnoreCase(type)) {
						output.collect(new Text(ids.get(splits[0]) + "\t"
								+ splits[1]), new Text(""));
					}
				} else {
					List<Integer> citations = new ArrayList<Integer>();
					for (int i = (3 + noOfAuthors + 2); i < attributes.length - 1; i++) {
						if (!attributes[i].equals("...")) {
							citations.add(ids.get(attributes[i]));
						}
					}
					String attribute = attributes[0] + "|" + attributes[1]
							+ "|" + attributes[2] + "|" + noOfAuthors + "|";
					for (int i = 1; i <= noOfAuthors; i++) {
						attribute += (attributes[3 + i] + "|");
					}
					attribute += (citations.size() + "|");
					for (int temp : citations) {
						attribute += (temp + "|");
					}
					attribute += (attributes[attributes.length - 1] + "|");
					output.collect(new Text(ids.get(splits[0]) + "\t" + attribute),
							new Text(""));
				}
			} catch (Exception e) {

			}
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new ExtractArticle(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Extract Data");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ExtractArticleMap.class);
		job.set("type", args[2]);
		job.setReducerClass(IdentityReducer.class);
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
