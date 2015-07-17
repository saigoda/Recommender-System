package heterogeneous.brainStorm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author sai bharath/rohitp
 * 
 *         This class is used to combine multiple networks into one single
 *         network by adding the itemCounts and the commonItems counts for each
 *         user from multiple networks.
 * 
 *         The code takes as input multiple paths as input and merges them int0
 *         a single output
 * 
 * 
 */
public class CombineMultipleNetworks extends Configured implements Tool {

	public static class CombinedWeightsOfNetworksMap extends MapReduceBase
			implements Mapper<Text, Text, Text, Text> {

		@Override
		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		/**
		 * <pre>
		 * Input -
		 *    key - userID:itemsClicked
		 *    value - {userID2:itemsClicked:commonItems,userID3:itemsClicked:commonItems ... userIDn:itemsClicked:commonItems}
		 * Output - 
		 *    key - userID
		 *    value - userID2:itemsClicked:commonItems,userID3:itemsClicked:commonItems ... userIDn:itemsClicked:commonItems
		 * </pre>
		 */
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {

			String[] user_degree = key.toString().split("\\:");

			String user = user_degree[0];
			output.collect(new Text(user),new Text(value.toString()));

		}

	}

	public static class CombinedWeightsOfNetworksReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		/**
		 * <pre>
		 * Input -
		 *   key - userID
		 *    value - userID2:itemsClicked:commonItems,userID3:itemsClicked:commonItems ... userIDn:itemsClicked:commonItems
		 *    		  userID2:itemsClicked:commonItems,userID3:itemsClicked:commonItems ... userIDn:itemsClicked:commonItems
		 *    
		 *    (At the value, we will get multiple neighborhoods for the same user from multiple networks.)
		 *    
		 * Output - 
		 *    key - userID:NA
		 *    value - userID2:NA:commonItems,userID3:NA:commonItems ... userIDn:itemsClicked:commonItems
		 * </pre>
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


			StringBuilder sb = new StringBuilder();
			
			/*HashMap to check for a user in the neighborhood from multiple networks and add the common item Count*/
			HashMap<String, Long> neighbors = new HashMap<String, Long>();
			while (values.hasNext()) {
				
				String[] splits = values.next().toString().split(",");
				
				for (String neighbor : splits) {//neighbor:0:10
					
					String[] neighborSplits = neighbor.split("\\:");//neighbor , 0 ,10
					String neighborKey = neighborSplits[0].trim();
					
					if (neighbors.containsKey(neighborKey)) {//if it has neighbor
						
						long neighborValue = neighbors.get(neighborKey);//get the commonItemCount associated with this key
						String upddatedneighborValue = "";
						
						long updatedCommonItemCount = neighborValue+ Long.parseLong(neighborSplits[2]);
						
						
						neighbors.put(neighborKey, updatedCommonItemCount);
						
					} else {
						neighbors.put(neighborKey,  Long.parseLong(neighborSplits[2].trim()));
					}
				}
				
			}
			
			/*Once we processed all the neighbor information from multiple networks, iterate through the hashmap and emit them */
			
			Iterator<String> it = neighbors.keySet().iterator();
			while (it.hasNext()) {
				String neighborKey = it.next();
				long neighborValue = neighbors.get(neighborKey);
				sb.append((neighborKey + ":NA:" + neighborValue) + ",");
			}
			
			output.collect(new Text(key.toString() + ":NA"),new Text(sb.toString()));
		}
	}

	/*public static class PrettyPrintReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		*//**
		 * <pre>
		 * Input -
		 *   key - userID:itemsClckedSummation
		 *   value - {userID2:itemsClickedSummation:commonItemsSummation} ... {userIDn:itemsClickedSummation:commonItemsSummation}
		 * Output - 
		 *    key - userID:itemsClicked
		 *    value - {userID2:itemsClicked:commonItems,userID3:itemsClicked:commonItems ... userIDn:itemsClicked:commonItems}
		 * </pre>
		 *//*
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringBuilder st = new StringBuilder();

			while (values.hasNext()) {
				st.append(values.next().toString() + ",");
			}

			output.collect(key, new Text(st.toString()));

		}
	}*/

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new CombineMultipleNetworks(),
					args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * args[0] - to args[length-3] - Input paths for multiple networks
	 * args[length-2] = Output path
	 * args[length-1] = numReducers
	 */
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {

		System.out.println("Running the first step of concatenation ");
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		
		job.setJobName("Combine counts of n networks");
		job.setJobPriority(JobPriority.NORMAL);
		
		job.setMapperClass(CombinedWeightsOfNetworksMap.class);
		job.setReducerClass(CombinedWeightsOfNetworksReduce.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[args.length-1]));
		
		job.set("mapred.reduce.child.java.opts", "-Xmx4096M");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		for (int i = 0; i <= args.length - 3; i++) {
			
			System.out.println("Input Path " + i + ":" + args[i]);
			
			MultipleInputs.addInputPath(job, new Path(args[i]),KeyValueTextInputFormat.class);
		}
		
		System.out.println("Output Path: " + args[args.length - 2]);
		
		FileSystem.get(job).delete(new Path(args[args.length - 2]));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 2]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}
}
