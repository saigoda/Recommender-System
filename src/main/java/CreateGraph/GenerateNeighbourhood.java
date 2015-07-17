package CreateGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
import org.utils.NearestNeighbourComparator;

public class GenerateNeighbourhood extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class GenerateNeighbourhoodMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter) value -- user:count \t
		 * item1,item2.... output:
		 */
		private FileSystem fs;

		private String outputPath;

		@Override
		public void configure(JobConf job) {
			try {
				fs = FileSystem.get(job);
				outputPath = job.get("directoryContainingLinkFiles");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String text = value.toString().trim();
			HashMap<String, Integer> userItemsCount = new HashMap<String, Integer>();
			try {
				String[] splits = text.split("\\t");
				String user_count = splits[0];
				splits[1] += ",";
				String[] items = splits[1].split(",");
				for (String item : items) {
					Path path = new Path(outputPath + "/" + item);
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(path)));
					String[] users = (br.readLine().split("\\t")[1] + ",")
							.split(",");
					br.close();
					for (String user : users) {
						if (user.equals(user_count))
							continue;
						else if (userItemsCount.containsKey(user)) {
							int count = userItemsCount.get(user);
							count++;
							userItemsCount.remove(user);
							userItemsCount.put(user, count);
						} else
							userItemsCount.put(user, 1);
					}
				}
				Iterator<String> keys = userItemsCount.keySet().iterator();
				StringBuilder sb = new StringBuilder();
				while (keys.hasNext()) {
					String userItem = keys.next();
					int count = userItemsCount.get(userItem);
					sb.append(userItem + ":" + count + ",");
				}
				output.collect(new Text(user_count), new Text(sb.toString()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class GenerateNeighbourhoodReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		private int _nearestNeighbours;

		@Override
		public void configure(JobConf conf) {
			_nearestNeighbours = Integer.parseInt(conf
					.get("numberOfNeighbours"));
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter) key - Item value --
		 * user:count,user:count.. Output --Files with item name are created and
		 * they store the user information
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			PriorityQueue<String> queue = new PriorityQueue<String>(100000,
					new NearestNeighbourComparator());
			String[] splits = values.next().toString().split(",");
			for (String s : splits) {
				queue.add(s);
			}
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < _nearestNeighbours && i<queue.size(); i++) {
				sb.append(queue.remove() + ",");
			}
			output.collect(key, new Text(sb.toString()));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new GenerateNeighbourhood(),
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
		job.setJobName("Generate Neighbourhood consisting of " + args[3]
				+ " neighbours");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(GenerateNeighbourhoodMap.class);
		job.setReducerClass(GenerateNeighbourhoodReduce.class);
		job.setNumMapTasks(100);
		job.set("directoryContainingLinkFiles", args[1]);
		job.set("numberOfNeighbours", args[3]);
		job.setNumReduceTasks(30);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
