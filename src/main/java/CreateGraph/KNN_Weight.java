package CreateGraph;


import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.utils.VertexObject;


/**
 * 
 *This class is used to generate K nearest neighbors according to weight values. Usually the value of k will be around 50. This reduces
 *the dataset size and computational complexity. The input to this class is the output from Neighboorhood job.
 *
 *Input format:
 *
 *user:itemsclicked \t Nei1:commonItems,nei2:commonItems...Nein:commonItems
 *
 *
 *Output Format: 
 *
 *user:itemsclickd:sum \t Nei1:weight,Nei2:weight...Nein:weight
 * 
 * @author rohitp
 *
 */
public class KNN_Weight extends Configured implements Tool{
	
	
	public static String inputPath_;
	
	public static String outputPath_;
	
	public static String kNeighbors;
	
	public static int numReducers_;
	
	
	
	public static void configure(String[] args) throws IOException {

		inputPath_ = args[0];
		outputPath_ = args[1];
		kNeighbors = args[2];
		numReducers_ = Integer.parseInt(args[3].trim());
	}
	
	
	
	/**
	 * Function to call the mapreduce jobs in this class. 
	 */
	public int run(String[] args) throws Exception {
		System.out.println("\n*****Loading the Configuration for this Job*****\n");
		configure(args);
		System.out.println("\n*****Done....*****\n");
        
        System.out.println("Computing the KNN and weights");
        JobConf conf1 = createJob1(args);        
 		RunningJob job1 = JobClient.runJob(conf1);
        System.out.println("\n*****Done....*****\n");
        
		return 0;
	}
	
		
	public static JobConf createJob1(String[] args) throws Exception
	{
		JobConf jobConf = new JobConf(KNN_Weight.class);

		jobConf.setJarByClass(KNN_Weight.class);
		jobConf.setJobName("KNN_Weights");
		
		
	    jobConf.setInputFormat(KeyValueTextInputFormat.class);
	    jobConf.setOutputFormat(TextOutputFormat.class);
		
				
		System.out.println("Input Path on HDFS: " +inputPath_); 
		System.out.println("Output Path on HDFS: " + outputPath_);
		
		
		/*Pass the mappingsFile to mapper*/
		
		jobConf.setNumReduceTasks(numReducers_);
		
		jobConf.set("numNeighbors", kNeighbors);
		
		Path usersInput = new Path(inputPath_);
		FileInputFormat.setInputPaths(jobConf,usersInput);

		FileSystem.get(jobConf).delete(new Path(outputPath_), true);
		FileOutputFormat.setOutputPath(jobConf, new Path(outputPath_));
						
		jobConf.setMapperClass(KNN_Weights_Mapper.class);
		jobConf.setReducerClass(IdentityReducer.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
        
        jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		

	    return jobConf;		
	}
	
		
	/**
	 * Second
	 * @author rohitp
	 *
	 */
	public static final class KNN_Weights_Mapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
			
		public static int kNeighbors_; 
		
		public void configure(JobConf jobConf) {
			
			kNeighbors_ = Integer.parseInt(jobConf.get("numNeighbors"));
			
		}
		
		
		/**
		 * Class to compare two vertices based on the weight property
		 * @author rohit
		 *
		 */
		public class VertexWeightComparator implements Comparator<VertexObject>
		{
			public int compare(VertexObject x, VertexObject y)
		    {
		        // Assume neither string is null. Real code should
		        // probably be more robust
		        if (x.numCommonItems < y.numCommonItems)
		        {
		            return -1;
		        }
		        if (x.numCommonItems > y.numCommonItems)
		        {
		            return 1;
		        }
		        return 0;
		    }
		}
		
		
		/**
		 * Function to add the vertex objects to the priority queue.
		 * @param queue
		 * @param obj
		 * @param size
		 */
		public static void addToQueue(PriorityQueue<VertexObject> queue, VertexObject obj)
		{
			if(queue.size() == kNeighbors_)
			{
				VertexObject head = queue.peek();
				
				if(obj.numCommonItems > head.numCommonItems)
				{
					queue.poll();
//					System.out.println("Removing " + head.vertexName + " :-:" + head.numCommonItems + " to queue");
					queue.add(obj);
//					System.out.println("Adding " + obj.vertexName + " :-:" + obj.numCommonItems + " to queue");
				}
			}
			else
			{
				queue.add(obj);
				
//				System.out.println("Adding " + obj.vertexName + " :-:" + obj.numCommonItems + " to queue");
				
			}
		}
		
		/**
         * <pre>
         * Input -
         *    key - userID1:itemcClicked
         *    value - {userID2:commonItems,userID3:commonItems ... userIDn:commonItems}
         * Output - 
         *    key - userID1:itemcClicked:WeightSum
         *    value - {userID2:weight,userID3:weight, ... userIDk:weight}
         * </pre>
         */

		public void map(Text key, Text value, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {
			
			try
			{
				
				Comparator<VertexObject> comparator = new VertexWeightComparator();
				
				PriorityQueue<VertexObject> queue = new PriorityQueue<VertexObject>(kNeighbors_, comparator);
				
				
				/*values will be just one.*/
				
				String neighbors = value.toString();
				
				
				String[] temp = neighbors.split(",");
				
				for(int i=0;i<temp.length;i++)
				{
					String user_commonCount = temp[i].trim();
					
					String[] temp1 = user_commonCount.split("\\:");
					
					VertexObject obj = new VertexObject();
					
					obj.vertexName = temp1[0].trim();
					obj.numCommonItems = Integer.parseInt(temp1[1].trim());
					
					/*Add it to the queue*/
					
					addToQueue(queue,obj);
					
				}
				
				double sum  = 0.0;
				StringBuilder st = new StringBuilder();
				
				Iterator<VertexObject> it = queue.iterator();
				
				while(it.hasNext())
				{
					VertexObject obj = it.next();
					sum = sum+obj.numCommonItems;
				}
				
				while(queue.size() != 0)
				{
					VertexObject obj = queue.remove();
					
					double weight = obj.numCommonItems/sum;
					
					st.append(obj.vertexName+":"+weight+",");
						
				}
				
				
				collector.collect(new Text(key.toString()+":"+sum), new Text(st.toString()));
				
				
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		
	}
	
	
	public static void main(String args[]) throws Exception
	{
		System.out.println("\nRunning the K Neighborhood creation job ***Updated***\n");
		ToolRunner.run(new Configuration(), new KNN_Weight(), args);
	}
	

}
