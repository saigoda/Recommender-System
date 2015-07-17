package heterogeneous.brainStorm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
	 *            This program concatenates both the networks and neighborhoods
	 *            so that it is easy to compute flow values. This class takes
	 *            the updated neighborhoods(neighborhoods updated with the
	 *            type of link information.
 * @author rohitp
 *
 */
public class ConcatenateNetworksForFlow extends Configured implements Tool {

	/**
	 * @param args
	 * 
	 * 
	 * 
	 */

	public static class ConcatenateNetworksForFlowMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 */ 
		 /** Input:
		 * 
		 * Key:<user:updateddegree>
		 * 
		 *  --- updatedDegree = degree+(typeofnetwork); --- 
		 * 
		 * Value--<user1:degree1:commonItemCountDist>,<user3:degree3:commonItemCountDist>,<user2:degree2:commonItemCountDist>,typeofnetwork
		 * 
		 * _typeofnetwork---Is the type of the network that is specified in the updated neighborhood job
		 * 
		 * Output:
		 * 
		 * Key--<User>
		 * 
		 * Values_1 -- updatedDegree 
		 * Values_2 -- <user1:degree1:commonItemCountDist>,<user3:degree3:commonItemCountDist>,<user2:degree2:commonItemCountDist>,typeofnetwork
		 */

		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			
			
			String keyString = key.toString();
			String[] userDegree = keyString.split("\\:");

			output.collect(new Text(userDegree[0]), new Text(userDegree[1]));
			output.collect(new Text(userDegree[0]), value);
		}
	}

	public static class ConcatenateNetworksForFlowReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */
		
		private HashMap<Integer, Integer> degrees = new HashMap<Integer, Integer>();
		private List<Integer> networkType = new ArrayList<Integer>();

/*		// remove the last character and test the rest of the string to see of
		// it is the degree or neighbors. If they are neighbors then the try
		// block will throw an exception*/
		public boolean isDegreeOrNeighbor(String source) {
			
			
			/*MyCode*/
			
			if(source.contains(":"))
			{
				return false;
			}
			else
			{
				int networkID = Integer.parseInt(source.charAt(source.length() - 1) + "");
				
				int degree = Integer.parseInt(source.substring(0, source.length() - 1));
				
				for (int network : networkType) {
					if (networkID == network)
					{
						/*store the degree information in the hashmap*/
						degrees.put(network, degree);
					}
				}

				return true;
			}

		}

		@Override
		public void configure(JobConf job) {
			try {
				int numNetworks = Integer.parseInt(job.get("numNetworks"));
				for (int i = 0; i < numNetworks; i++) {
					networkType.add(Integer.parseInt(job.get("type" + (i + 1))));
				}
			} catch (Exception e) {

			}
		}

		/** Input:
		 * 
		 * Key--<User>
		 * 
		 * ValueType_1 -- updatedDegree1,updatedDegree2 ... updatedDegree_numNetworks 
		 * ValueType_2 -- Neighbor information from n networks
		 * 
		 * <user1:degree1:commonItemCountDist>,<user3:degree3:commonItemCountDist>,<user2:degree2:commonItemCountDist>,typeofnetwork
		 * 		
		 * Output:
		 * 
		 * Key:<User:updatedDegree1:updatedDegree2>
		 * 
		 * Value--<user1:degree1:commonItemCountDist1:degree2:commonItemCountDist2>,
		 * 		  <user3:degree1:commonItemCountDist1:degree2:commonItemCountDist2>,
		 *        <user2:degree1:commonItemCountDist1:degree2:commonItemCountDist2>,
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			
			/*HashMap to store the neighbor information: The key is the  neighbor userID*/
			HashMap<String, HashMap<Integer, String>> neighbors = new HashMap<String, HashMap<Integer, String>>();
			
			while (values.hasNext()) {
				
				/*The input value can be either the degree (itemCount) or neighborhood information*/
				String itemCountOrNeighborsString = values.next().toString().trim();
				
				
				boolean itemCount = isDegreeOrNeighbor(itemCountOrNeighborsString);
				
				// If the value is not an itemCount then go to this part of the code and store the neighbor information
				if (!itemCount) {
					

					/*split the string using comma and store the neighbor information in a hashmap*/
					
					String[] networkNeighborString = itemCountOrNeighborsString.split(",");
					
					/*The last value corresponds to the network ID*/
					int networkID = Integer.parseInt(networkNeighborString[networkNeighborString.length-1]);
					
					/*iterate through the neighbors*/
					for(int i=0;i< networkNeighborString.length-1;i++)
					{
						/*The string array has userID, itemCount and commonItemDistribution*/
						String[] neighborInfo = networkNeighborString[i].trim().split(",");
						
						for (String s : neighborInfo) {
							
							// store all the edges in a hash map.
							String[] splits = s.split("\\:");
							
							// The key of the hashmap is the node name and the values are the itemCount and commonItemDistribution in each network respectively.
							// If the node is not present in both the neighborhoods we store only one neighbors degree and weight
							if (splits.length == 3)
							{
								s = splits[0];// splits[0]--contains the node name
								if (neighbors.containsKey(s)) 
								{
									
									HashMap<Integer, String> map = neighbors.get(s);
									
									for (int network : networkType)
									{
										
										/*If the current network ID matches the current network type*/
										if (networkID == network)
										{
											map.put(network, (splits[1]+ ":"+ splits[2]+ ":"));
											break;
										}
									}
									neighbors.put(s, map);
								} 
								else
								{
									for (int network : networkType) 
									{
										if (networkID == network)
										{
											HashMap<Integer, String> map = new HashMap<Integer, String>();
											map.put(network, (splits[1]+ ":"+ splits[2]+ ":"));
											neighbors.put(s, map);
											break;
										}
									}
								}

							}
						}
					}
					
				}
			}
			
			String updatedKey = key.toString();
			for (int network : networkType) {
				
				if(degrees.containsKey(network))
					updatedKey += ":" + degrees.get(network);
				else
					updatedKey += ":" + 0;
			}

			
			/*So far, we have parsed the itemCount information from different networks for the key user and stored it. 
			 * We also parsed the neighbor information from multiple networks and stored it. Now we have to print it.*/
			

			// Iterate through the hashmap and store all the values(degree and
			// weights) of the hashmaps without the appended characters
			// The order of storing an is :
			/*
			 * If there is an edge of type ingredient and recipe in the
			 * neighborhood then we store the edge as:
			 * 
			 * neighborID:recipe_degree:recipe_common_neighbors:ingredient_degree:ingredient_common_neighbors
			 * 
			 * If there is recipe edge and no ingredient edge then we store the edge as:
			 * 
			 * neighborID:recipe_degree:recipe_common_neighbors:0:0
			 * 
			 * If there is a ingredient edge and no recipe edge then we store the edge as:
			 * 
			 * neighborID:0:0:ingredient_degree:ingredient_common_neighbors
			 */
			Iterator<String> neighborIterator = neighbors.keySet().iterator();
			StringBuffer edges = new StringBuffer();
			
			while (neighborIterator.hasNext()) {
				String neighborKey = neighborIterator.next();
				/*Get the hashmap with the networkID as key and itemCount:dist as value*/
				HashMap<Integer, String> neighborValue = neighbors.get(neighborKey);
				
				String degreeWeight = "";
				
				for (int network : networkType) {
					if (neighborValue.containsKey(network)) {
						degreeWeight += (neighborValue.get(network));
					} else {
						degreeWeight += ("0:0:");
					}
				}
				edges.append(neighborKey + ":" + degreeWeight.substring(0, degreeWeight.length() - 1)+ ",");
			}

			// emit the node and it's new properties
			output.collect(new Text(updatedKey), new Text(edges.toString()));
			
			/*Degrees hashmap has to be reset for every user*/
			degrees.clear();
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new ConcatenateNetworksForFlow(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Concatenate Degrees and Networks");
		job.setJobPriority(JobPriority.NORMAL);
		
		job.setMapperClass(ConcatenateNetworksForFlowMapper.class);
		job.setReducerClass(ConcatenateNetworksForFlowReducer.class);

		job.setOutputFormat(TextOutputFormat.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[args.length-1]));
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		int numNetworks = Integer.parseInt(args[args.length - 2]);
		
		for (int i = 0; i < numNetworks; i++) {
			
			System.out.println("Input path is: " + args[i]);
			MultipleInputs.addInputPath(job, new Path(args[i]),KeyValueTextInputFormat.class);
		}
		
		job.set("numNetworks", numNetworks+"");
		
		
		String outputPath_ = args[numNetworks];
		
		System.out.println("Output path is: " + outputPath_);
		
		FileSystem.get(job).delete(new Path(outputPath_));		
		FileOutputFormat.setOutputPath(job,new Path(outputPath_));
		
		for (int i = 0; i < numNetworks; i++) {
			
			/* set the type of the network that we are using to store the neighborhood. The same type is to be used throughout */
			
			System.out.println("type" + (i + 1));
			System.out.println(args[numNetworks+1+i]);
			
			job.set("type" + (i + 1), args[numNetworks+1+i]);
		}
		
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
