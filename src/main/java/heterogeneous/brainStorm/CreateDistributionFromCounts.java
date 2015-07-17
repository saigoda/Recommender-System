package heterogeneous.brainStorm;

import java.io.IOException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class is used to modify the input from common item count into distributions. For example, consider the following input format:
 * 
 * <User:itemCount>\t<User1:itemCount:commonItems>,<User2:itemCount:commonItems>,<User3:itemCount:commonItems>
 * 
 * <User1:10>\t<User1:5:2>,<User2:8:3>,<User3:6:3> => <User1:10>\t<User1:5:2/(2+3+3)>,<User2:8:3/(2+3+3)>,<User3:6:3/(2+3+3)>
 * 
 * @author rohitp
 *
 */
public class CreateDistributionFromCounts extends Configured implements Tool{
	
	
	
	/*Path to input folder*/
	private String inputPath_;	
	/*Path to distribution of output folder*/
	private String outPath_;
	
	public void configure(String[] args) throws IOException {

		inputPath_ = args[0];
		outPath_ = args[1];
	}
	
	
	
	@Override
	/**
	 * Function to call the mapreduce jobs in this class. 
	 */
	public int run(String[] args) throws Exception {
		System.out.println("\n*****Loading the Configuration for this Job*****\n");
		configure(args);
		System.out.println("\n*****Done....*****\n");
		
		
        JobConf conf0 = createJob0(args);
 		RunningJob job0 = JobClient.runJob(conf0);
        System.out.println("\n*****Done....*****\n");

        
		return 0;
	}
	
	/**
	 * Method to set the configuration of the MapReduce Job 
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public JobConf createJob0(String[] args) throws Exception
	{
		JobConf jobConf = new JobConf(getConf(), this.getClass());

		jobConf.setJarByClass(CreateDistributionFromCounts.class);
		jobConf.setJobName("DistributionGenerationJob");
		
		
	    jobConf.setInputFormat(KeyValueTextInputFormat.class);
	    jobConf.setOutputFormat(TextOutputFormat.class);
		
				
		System.out.println("Input Path on HDFS: " +inputPath_); 
		System.out.println("Output Path on HDFS: " + outPath_);
		
		Path clicksInput = new Path(inputPath_);
		FileInputFormat.setInputPaths(jobConf,clicksInput);

		FileSystem.get(jobConf).delete(new Path(outPath_), true);
		FileOutputFormat.setOutputPath(jobConf, new Path(outPath_));
		
		
		jobConf.set("mapred.map.child.java.opts", "-Xmx4096M");
		
		
		jobConf.setNumReduceTasks(0);
						
		jobConf.setMapperClass(DistributionMapper.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
        
        jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		

	    return jobConf;		
	}
	
	
	public static final class DistributionMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
		/**
         * <pre>
         * Input -
         *    key - <User:itemCount>
         *    value - <User1:itemCount:commonItems>,<User2:itemCount:commonItems>,<User3:itemCount:commonItems>
         * Output - 
         *    key - <User:itemCount>
		 *    value - <User1:itemCount:commonItemDistribution>,<User2:itemCount:commonItemDistribution>,<User3:itemCount:commonItemDistribution>
         * </pre>
         */

		public void map(Text key, Text value, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {
			
			
			/*split the value by comma and first sum the commonItem couts and then re iterate and divide*/
			
			String valueString = value.toString();
			
			if(!(valueString.equalsIgnoreCase("") || valueString.equalsIgnoreCase(null)))
			{
				
				System.out.println("ComingInside");
				String[] neighbors = value.toString().trim().split(",");
				
				/*iterate and compute the sum*/
				double sum = 0;
				
				for(int i = 0;i<neighbors.length;i++)
				{
					String neighbor = neighbors[i];
					String[] temp =  neighbor.split("\\:");
					
					double numCommonItems =  Double.parseDouble(temp[2]);
					
					sum = sum + numCommonItems;
					
				}
				
				/*Now that the sum is computed, we have to normalize each neighbor with the sum*/
				
				StringBuilder st = new StringBuilder();

				for(int i = 0;i<neighbors.length;i++)
				{
					String neighbor = neighbors[i];
					String[] temp =  neighbor.split("\\:");
					
					double dist = Double.parseDouble(temp[2])/sum;
					
					String newNeighbor = temp[0]+":"+temp[1]+":"+dist;
					
					st.append(newNeighbor+",");
					
				}
				
				collector.collect(key, new Text(st.toString()));
			}
		}
		
	}
	
	
	public static void main(String[] args) throws Exception
	{
		System.out.println("\nCreating a distribution for the common item count\n");
        ToolRunner.run(new Configuration(), new CreateDistributionFromCounts(), args);
	}

}
