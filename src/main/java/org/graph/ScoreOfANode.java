package org.graph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
import org.utils.Node;

public class ScoreOfANode extends Configured implements Tool {

	/**
	 * @param args
	 * 
	 *            This class takes the BFS final iteration and then calculates
	 *            the score
	 * 
	 *            The score of a node is determined by the formula
	 * 
	 *            Score(v,u) = 1 - (1 - 1/degree(u))^weight(u,v)
	 */

	public static class ScoreOfANodeMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input:
		 * 
		 * Key:---<user:degree>
		 * 
		 * Value:----<user1:degree1:commonneighbors1>,<user3:degree3:
		 * commonneighbors3
		 * >,<user2:degree2:commonneighbors2>|distance|Color|parent
		 * 
		 * Output:
		 * 
		 * Key--<parent,user>
		 * 
		 * Value--common neighbors of <parent,user>
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			Node node = new Node(value.toString().trim());
			if (node.getParent() != null
					&& !node.getParent().equalsIgnoreCase("null")
					&& node.getColor().toString().equals("BLACK")) {
				// check if node's parent is already been visited in the tree or
				// if the parent is not null
				String source = node.getParent();// The parent is the source of
													// the node
				String dest = node.getId();// The node itself is the destination
											// that is reached from the parent
				String val = "";
				for (String edge : node.getEdges()) {
					if (edge.contains(source)) {// Search for the parent in the
												// neighborhood of the node. The
												// parent should be definitely
												// be there in the
												// neighbourhood.From the
												// neighborhood get the weight
												// of the parent.
						val = edge.split(":")[2].trim();
						if (val.length() > 0)
							break;
					}
				}
				output.collect(new Text(source + "," + dest), new Text(val));
			}
		}
	}

	public static class ScoreOfANodeReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter) 
		 * 
		 * Input:
		 * 
		 * Key--<parent,user>
		 * 
		 * Value--common neighbors of <parent,user>
		 * 
		 * Output:
		 * 
		 * Key--<parent,user>
		 * 
		 * Value--score of <parent,user>
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			System.out.println("Key = " + key.toString());
			int weight = 0;
			while (values.hasNext()) {
				try {
					weight = Integer.parseInt(values.next().toString());
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}
			System.out.println("Weight = " + weight);
			if (weight > 0) {
				String[] splits = key.toString().split(",");
				String dest = splits[1];
				int destDegree = Integer.parseInt(dest.split(":")[1]);//get the degree of the node
				double val = 1 / (double) destDegree;
				double score = 1 - Math.pow((1 - val), weight);//calculate 1-(1-1/degree(u))^weight(u,v)
				output.collect(key, new Text(score + ""));//emit the <parent,node> and score
			}
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new ScoreOfANode(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Score of a Node");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(ScoreOfANodeMap.class);
		job.setReducerClass(ScoreOfANodeReduce.class);
		// job.setNumMapTasks(100);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		Path inputPath = new Path(args[0]);
		String input = args[0];
		FileSystem.get(job).delete(new Path(args[1]));
		FileSystem fs = FileSystem.get(job);
		FileStatus[] status = fs.listStatus(inputPath);
		System.out.println("Status length is:" + status.length);
		input = input + "/" + (status.length + "");
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
