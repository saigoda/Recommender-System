package heterogeneous.brainStorm;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UpdateNetworkNeighbourhood extends Configured implements Tool {

	/**
	 * @param args
	 * 
	 *            This program takes the neighborhood of a network in the form of a distribution and adds the type of network to the data.	 */


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
		 * key -- <user:degree>
		 * value -- <user1:degree1:commonneighbors1>,<user3:degree3:commonneighbors3>,<user2:degree2:commonneighbors2>
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

//		public void map(LongWritable key, Text value,
//				OutputCollector<Text, Text> output, Reporter reporter)
//				throws IOException {
//			// TODO Auto-generated method stub
//
//			Node node = new Node(value.toString().trim());
//			output.collect(new Text(node.getId()), node.getNodeInfo());
//		}
//	}

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

		/**
		 * * Input:
		 * 
		 * key -- <user:degree>
		 * value -- <user1:degree1:commonItemCountDist>,<user3:degree3:commonItemCountDist>,<user2:degree2:commonItemCountDist>,
		 * 
		 * Output:
		 * 
		 * Key:<user:updateddegree>
		 * 
		 *  --- updatedDegree = degree+(typeofnetwork); --- 
		 * 
		 * Value--<user1:degree1:commonItemCountDist>,<user3:degree3:commonItemCountDist>,<user2:degree2:commonItemCountDist>,typeofnetwork
		 * 
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			
			StringBuilder st = new StringBuilder();
			
			while (values.hasNext()) {
				st.append(values.next().toString()+_typeOfNetwork);
			}
			
			String[] keySplits = key.toString().split("\\:");
			
			String updatedKey = keySplits[0] + ":"+ keySplits[1]+ _typeOfNetwork.toLowerCase().charAt(0);
			

			output.collect(new Text(updatedKey), new Text(st.toString()));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new UpdateNetworkNeighbourhood(), args);
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
		job.setJobPriority(JobPriority.NORMAL);
		
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(UpdateNetworkNeighbourhoodReduce.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		System.out.println("The input path is: " + args[0]);
		System.out.println("The output path is: " + args[1]);
		
		
		Path inputPath = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inputPath);
		
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.set("typeOfNetwork", args[2]);
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
