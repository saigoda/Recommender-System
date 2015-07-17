package CreateGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

public class UpdateNetworkNeighbourhood extends Configured implements Tool {

	/**
	 * @param args
	 * 
	 *            This program takes the last iteration result of the BFS
	 *            algorithm and updates the neighborhood to include the network
	 *            type as well
	 */

	public static class UpdateNetworkNeighbourhoodMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

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
		 * <user:degree>\t<user1:degree1:commonneighbors1>,<user3:degree3:
		 * commonneighbors3
		 * >,<user2:degree2:commonneighbors2>|distance|Color|parent
		 * 
		 * Output:
		 * 
		 * Key:---<user:degree>
		 * 
		 * Value:----<user1:degree1:commonneighbors1>,<user3:degree3:
		 * commonneighbors3
		 * >,<user2:degree2:commonneighbors2>|distance|Color|parent
		 * 
		 * The mapper does not do anything. It just emits the key value pairs
		 * from the input file
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			Node node = new Node(value.toString().trim());
			output.collect(new Text(node.getId()), node.getNodeInfo());
		}
	}

	public static class UpdateNetworkNeighbourhoodReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * 
		 * The reducer has the type of network as it's parameter which is
		 * appended to the output
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
		 * Key:<user:updateddegree>
		 * 
		 * updatedDegree--degree+_typeofnetwork.toLowerCase().charAt(0);
		 * 
		 * Value--<user1:degree1:updatedcommonneighbors1>,<user3:degree3:
		 * updatedcommonneighbors3
		 * >,<user2:degree2:updatedcommonneighbors2>|distance|Color|parent
		 * 
		 * updatedcommonneighbors(i)--commonneighbors(i)+_typeofnetwork.toLowerCase
		 * ().charAt(0);
		 */
		private String _typeOfNetwork;

		@Override
		public void configure(JobConf job) {
			try {

				_typeOfNetwork = job.get("typeOfNetwork");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer buffer = new StringBuffer();
			while (values.hasNext()) {
				buffer.append(values.next().toString());
			}
			String updatedKey = key.toString().split(":")[0] + ":"
					+ key.toString().split(":")[1]
					+ _typeOfNetwork.toLowerCase().charAt(0);
			List<String> newEdges = new ArrayList<String>();
			Node node = new Node(updatedKey + "\t" + buffer.toString());
			for (String s : node.getEdges()) {
				String[] splits = s.split(":");
				if (splits.length == 3) {
					String weight = splits[2] + ""
							+ _typeOfNetwork.toLowerCase().charAt(0);
					s = splits[0] + ":" + splits[1] + ":" + weight;
					newEdges.add(s);
				}
			}
			node.setEdges(newEdges);
			node.setId(updatedKey);
			node.setParent(node.getParent()
					+ _typeOfNetwork.toLowerCase().charAt(0));
			output.collect(new Text(node.getId()), node.getNodeInfo());
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new UpdateNetworkNeighbourhood(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Update Network Neighbourhood");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(UpdateNetworkNeighbourhoodMap.class);
		job.setReducerClass(UpdateNetworkNeighbourhoodReduce.class);
		// job.setNumMapTasks(100);
		job.setNumReduceTasks(30);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		Path inputPath = new Path(args[0]);
		String input = args[0];
		FileSystem fs = FileSystem.get(job);
		FileStatus[] status = fs.listStatus(inputPath);
		input = input + "/" + status.length;
		FileInputFormat.setInputPaths(job, new Path(input));
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.set("typeOfNetwork", args[2]);
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
