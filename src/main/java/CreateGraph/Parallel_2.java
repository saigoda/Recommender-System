package CreateGraph;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.utils.VertexObject;
/**
 *This code is advanced to Parallel.java. This splits the estimated label creation job into 2 jobs and thus improves the speed. 
 *This is for the variant 1 where each line in the neighborhood is of the following format:
 *
 * userID:itemsClicked:weightSum	neighborUser:weight
 * 232011:12:15.0  251237:0.2,150384:0.2,137183:0.2,357635:0.2,181914:0.2,
 * 
 * 
 * The format of the input is a line from the labeldist file
 * 
 * 
 * userID,itemID,labelDist
 *
 *
 *	Tested and it works correctly based on comparison of the second iteration result from the output with using Titan graph. 
 *
 *TODO:
 *
 * 1. Make sure that the number of files in the KNN folder is small. This program reads those files into memory, so it is better that we open
 * few files.
 * 2. Make sure that the heapspace is sufficiently large when the number of users is large. 2GB might be fine for 1 million users. Might need
 * few more optimizations after that. 
 * 3. Make sure that the output from the previous iteration is deleted after being used. To be safe for iteration i, delete i-2 as we will 
 * currently use folder i-1.
 *
 * 
 * @author rohitp
 *
 */
public class Parallel_2{
	
	/*Name of the database to query and update*/
	public static String dbName_;
	
	public static double mu1 = 1, mu2 =1, mu3 = 1;
	
	public static int keepTopKLabels_;
	
	/*Number of iterations to run the adsorption algorithm*/
	public static int numIterations_;
	
	public static String userLabelDistPath_;
	
	public static String userNeighborInputPath_;
	
	public static String edgeBaseOutputPath_;
	
//	public static String host_;
	
	public static int num_reduceTasks_=1;
	
	public static String paramString_;
	
	public static HashMap<String, HashMap<String, String>> cvParams  = new HashMap<String, HashMap<String, String>>();
	
	public static HashMap<String, LinkedList<Double>> probabilities = new HashMap<String, LinkedList<Double>>();
	
	public static double inj ;
	public static double cont;
	public static double term;
	
	/**
	 * Function to laod the cvParams for identifying the folds which has to used for cross validation.
	 * 
	 * For example,
	 * 
	 * 
	 * 0 => 0_1 as train, 2 as validation and 3 as testing.
	 * 1 => 1_2 as train, 3 as validation and 0 as testing.
	 * 2 => 2_3 as train, 0 as validation and 1 as testing.
	 * 3 => 3_0 as train, 1 as validation and 2 as testing.
	 */
	public static void loadcvParams()
	{
		for(int i=0;i<4;i++)
		{
			String cvNum = i+"";
			
			
			int trainFold1 = i;
			int trainFold2 = i+1;
			
			if(trainFold2 > 3)
				trainFold2=0;
			
			String trainCV = trainFold1+"_"+trainFold2;
			
			int validationFold = trainFold2 + 1;
			
			if(validationFold > 3)
				validationFold=0;
			
			String validationCV = validationFold+"";
			
			int testFold = validationFold+1;
			
			if(testFold > 3)
				testFold = 0;
			
			String testCV = testFold+"";
			
			HashMap<String, String> tr_va_te = new HashMap<String, String>();
			
			tr_va_te.put("Train", trainCV);
			
			tr_va_te.put("Validation", validationCV);
			
			tr_va_te.put("Test",testCV);
			
			cvParams.put(cvNum, tr_va_te);
			
		}
	}
	
	/**
	 * function to load the randomwalk probabilities.
	 * 
	 * 
	 * probable values to be used for the parameters. 
	 * 
	 * 
	 * #1) 0,0.7,0.0
	 	#2) 0,0.85,0.0
		#3) 0.0,1,0
		
		Best way is to create a string and split it rather than changing at each point.  
		
	 */
	public static void loadProbabilities()
	{
		
		String zeroParams = "0,0.7,0.0";
		String oneParams = "0,0.85,0.0";
		String twoParams = "0.0,1,0";
//		String threeParams = "0.05,0.9,0.05";
//		String fourParams = "0.1,0.9,0.0";
//		String fiveParams = "0.15,0.75,0.1";

		
		/*iteration 0*/
		LinkedList<Double> zero = new LinkedList<Double>();
		zero.add(0.0);
		zero.add(0.7);
		zero.add(0.0);
		probabilities.put("0", zero);
		
		/*Iteration 1*/
		LinkedList<Double> one = new LinkedList<Double>();
		one.add(0.0);
		one.add(0.85);
		one.add(0.0);
		probabilities.put("1", one);
		
		/*Iteration 2*/
		LinkedList<Double> two = new LinkedList<Double>();
		two.add(0.0);
		two.add(1.0);
		two.add(0.0);
		probabilities.put("2", two);
		
	}
	
	public static void configure(String[] args) throws IOException {

		userLabelDistPath_ = args[0];
		userNeighborInputPath_ = args[1];
		edgeBaseOutputPath_ = args[2];
		keepTopKLabels_ = Integer.parseInt(args[3]);
		numIterations_ = Integer.parseInt(args[4]);
		num_reduceTasks_ = Integer.parseInt(args[5]);
		paramString_ = args[6];
		
//		if(args[7].equalsIgnoreCase("local"))
//		{
//			host_ = "localhost";
//		}
//		else
//		{
//			host_ =  "10.50.0.2,10.50.0.4,10.50.0.5,10.50.0.6,10.50.0.7,10.50.0.8,10.50.0.9,10.50.0.10,10.50.0.11," +
//					"10.50.0.12,10.50.0.13,10.50.0.14,10.50.0.15,10.50.0.16,10.50.0.17,10.50.0.18,10.50.0.19,10.50.0.20,10.50.0.21,10.50.0.22";
//			host_ = "theia,titan01,titan02,titan04,titan05,titan06,titan07,titan08,titan09,titan10";
//		}
		
		
//		System.out.println("Host is: " + host_);
		
	}
	
	
	/**
	 * Job to change the input to the algorithm. The output is values in labelDist. However, instead of having a user,item interaction in each
	 * row, I will have it for each user.
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public static JobConf createJob0(JobConf jobConf) throws Exception
	{

		jobConf.setJarByClass(Parallel_2.class);
		jobConf.setJobName("PrepareInput");
		
	    jobConf.setInputFormat(TextInputFormat.class);
	    jobConf.setOutputFormat(TextOutputFormat.class);
	    
	    
	    Path usersInput = new Path(userLabelDistPath_);
		FileInputFormat.setInputPaths(jobConf,usersInput);
		System.out.println("Input Path on HDFS: " +userLabelDistPath_); 
	    
	    String outPath_ = edgeBaseOutputPath_+"input_";
		
		System.out.println("Output Path on HDFS: " + outPath_);
		
		jobConf.set("mapred.map.child.java.opts", "-Xmx1536M");
		
		
		jobConf.setNumReduceTasks(num_reduceTasks_);
		

		FileSystem.get(jobConf).delete(new Path(outPath_), true);
		FileOutputFormat.setOutputPath(jobConf, new Path(outPath_));
						
		jobConf.setMapperClass(PrepareInputMapper.class);
		jobConf.setReducerClass(PrepareInputReducer.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
        
        jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		

	    return jobConf;		
	}
	
	
	/**
	 * Method to compute the iteration process for adsorption. In this job, we compute the new edges to be added to the graph. 
	 * 
	 * Think about using multiple reducers i.e in effect we have to know if we can do parallel updates on a vertex.
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public static JobConf createJob1(int it) throws Exception
	{
		JobConf jobConf = new JobConf(Parallel_2.class);

		jobConf.setJarByClass(Parallel_2.class);
		jobConf.setJobName("ComputeAggregate_"+it);
		
	    jobConf.setInputFormat(KeyValueTextInputFormat.class);
	    jobConf.setOutputFormat(TextOutputFormat.class);
	    
//	    if(it>=1)
//	    {
//	    	String delPath = edgeBaseOutputPath_+"final_"+(it-1);
//	    	FileSystem.get(jobConf).delete(new Path(delPath), true);
//	    }
		
	    String outPath_ = edgeBaseOutputPath_+"final_"+it;
		
		/*Pass the mappingsFile to mapper*/
		jobConf.setInt("ITERATION", it);
		
		
		jobConf.set("Pinj", inj+"");
		jobConf.set("Pcont", cont+"");
		jobConf.set("Pterm", term+"");
		jobConf.set("neighbors",userNeighborInputPath_);
		jobConf.setInt("ITERATION", it);
		jobConf.setInt("keepTopKLabels", keepTopKLabels_);
		
		jobConf.set("mapred.child.java.opts", "-Xmx2048M");
		
		jobConf.setNumReduceTasks(num_reduceTasks_);
		
		if(it>=2)
	    {
	    	String delPath = edgeBaseOutputPath_+"final_"+(it-2);
	    	FileSystem.get(jobConf).delete(new Path(delPath), true);
	    }
		
		if(it == 0)
		{
			Path usersInput = new Path(edgeBaseOutputPath_+"input_");
			FileInputFormat.setInputPaths(jobConf,usersInput);
			System.out.println("Input Path on HDFS: " +edgeBaseOutputPath_+"input_"); 
		}
		else
		{
			Path usersInput = new Path(edgeBaseOutputPath_+"final_"+(it-1));
			FileInputFormat.setInputPaths(jobConf,usersInput);
			System.out.println("Input Path on HDFS: "+ edgeBaseOutputPath_+"final_"+(it-1)); 
		}

		System.out.println("Output Path on HDFS: " + outPath_);


		FileSystem.get(jobConf).delete(new Path(outPath_), true);
		FileOutputFormat.setOutputPath(jobConf, new Path(outPath_));
						
		jobConf.setMapperClass(LabelsMap.class);
		jobConf.setReducerClass(LabelsReducer.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
        
        jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		

	    return jobConf;		
	}
	
	
	public static JobConf createJob2(int it) throws Exception
	{
		JobConf jobConf = new JobConf(Parallel_2.class);

		jobConf.setJarByClass(Parallel_2.class);
		jobConf.setJobName("PrepareOutput");
		
	    jobConf.setInputFormat(KeyValueTextInputFormat.class);
	    jobConf.setOutputFormat(TextOutputFormat.class);
	    
	    
	    Path usersInput = new Path(edgeBaseOutputPath_+"final_"+it);
		FileInputFormat.setInputPaths(jobConf,usersInput);
		System.out.println("Input Path on HDFS: " +edgeBaseOutputPath_+"final_"+it); 
	    
	    String outPath_ = edgeBaseOutputPath_+"output_";
		
		System.out.println("Output Path on HDFS: " + outPath_);
		
		jobConf.set("mapred.map.child.java.opts", "-Xmx1536M");
		
		
		jobConf.setNumReduceTasks(num_reduceTasks_);
		

		FileSystem.get(jobConf).delete(new Path(outPath_), true);
		FileOutputFormat.setOutputPath(jobConf, new Path(outPath_));
						
		jobConf.setMapperClass(MyIdentityMapper.class);
		jobConf.setReducerClass(IdentityReducer.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
        
        jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		

	    return jobConf;		
	}
	

	public static final class PrepareInputMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		
		/**
         * <pre>
         * Input -
         *    key - longWritable
         *    value - userID,itemID,labelDist
         * Output - 
         *    key - userID
         *    value - itemID:labelDist 
         *    
         *    
         * </pre>
         */

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {
			
			
			String[] temp = value.toString().split(",");
			
			String keyReturn = temp[0].trim();
			String valueReturn = temp[1].trim()+":"+temp[2].trim();
			
			collector.collect(new Text(keyReturn), new Text(valueReturn));
			
		}
		
	}
	
	
	public static final class PrepareInputReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		/**
		 * <pre>
		 * Input -
		 *    key - userID
		 *    value - {itemID:labelDist ... itemID:labelDist}
		 * Output - 
		 *    key - userID
		 *    value - {itemID:labelDist, ... ,itemID:labelDist} as a string with comma seperation
		 * </pre>
		 */

		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			StringBuilder st = new StringBuilder();
			
			while(values.hasNext())
			{
				st.append(values.next().toString()+",");
			}
			
			output.collect(key, new Text(st.toString()));
			
		}
		
	}

		
	
	
	/**
	 * Class to emit the injected labels estimated labels for the users. Reads neighbors into memory and used labeldist file as input
	 * 
	 * The code assumes that Pinj is 0 and does not take into account the injected labels. However, if used with values Pinj > 0
	 * code has to be changed to accommodate that.
	 * 
	 * @author rohitp
	 *
	 */
	public static final class LabelsMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
		int it_;
		public static String neighbor_;
		public static double Pinj, Pcont;
		

		public static enum LARGE_ITEMSET {
			UserCounter,
		};
		
		

		/*HashMap to store the inverse neighbors. i.e if we have the following line in  the neighborhood file
		 * 
		 * v u,w,x,y,z
		 * 
		 * then in the hashmap we will have 5 keys and each key will have v as the value in its linkedlist. This is because to compute
		 * the estimated label dist for v, we need labels from u.
		 * 
		 * */
		public static HashMap<String,LinkedList<String>> inverseneighbors = new HashMap<String,LinkedList<String>>();
		
//		public static double Pinj, Pcont, Pterm;
		
		FileSystem fs;
		public void configure(JobConf jobConf) {

			it_ = jobConf.getInt("ITERATION", -1);
			neighbor_ = jobConf.get("neighbors");
			
			
			Pinj = Double.parseDouble(jobConf.get("Pinj"));
			Pcont = Double.parseDouble(jobConf.get("Pcont"));
//			Pterm = Double.parseDouble(jobConf.get("Pterm"));

			try {
				fs = FileSystem.get(jobConf);
				
				
				FileStatus[] status = fs.listStatus(new Path(neighbor_));
				
				 for (int i=0;i<status.length;i++){
					 
					 FileStatus fstatus = status[i];
					 
					 if(!fstatus.isDir())
					 {
						 /* Open the corresponding file */
							BufferedReader br = null;
							
							br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
							
							String line;
							/*9878:11:20.0    192342:0.2,309432:0.2,78243:0.2,47708:0.2,354215:0.2,*/
							while((line = br.readLine()) !=null)
							{
								String[] temp = line.split("\t");
								
								String userString = temp[0];
								String userID = userString.split("\\:")[0].trim();
								
								String[] temp1 = temp[1].split(",");
								
								for(String st : temp1 )
								{
									String[] temp2 = st.split("\\:");
									
									String user_v = temp2[0];
									
									LinkedList<String> list;
									
									if(!inverseneighbors.containsKey(user_v))
									{
										list = new LinkedList<String>();
										
										list.add(userID);
										list.add(temp2[1]);
										inverseneighbors.put(user_v, list);
									}
									else
									{
										list = inverseneighbors.get(user_v);
										list.add(userID);
										list.add(temp2[1]);
										inverseneighbors.put(user_v, list);
									}
								}
							}
					 }
				 }
				
				
				System.out.println("The size of the inverse neighborhood is: " + inverseneighbors.size());
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if(it_ < 0)
			{
				System.err.println("The value of the iteration is less than zero. Something is wrong");
				System.exit(-1);
			}
			
		}
		
				
		/**
         * <pre>
         * Input -
         *    key - userID
         *    value - {itemID:labelDist,itemID:labelDist, ... ,itemID:labelDist}
         * Output - 
         *    
         *    {(u),(0,Yu)} if Pinj > 0
         *    {(v), (1,itemID,Wuv*labelDist)}
         *    
         * </pre>
         */

		public void map(Text key, Text value, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {

			try {

				String user_u = key.toString();

				/*
				 * If this user is not in the inverseneighbor hashmap as
				 * key, then we need not compute it because no one is
				 * expecting any values from this
				 */
				if (inverseneighbors.containsKey(user_u)) {
					
					
					LinkedList<String> list = inverseneighbors.get(user_u);
					
					String itemsString = value.toString();

					String[] temp = itemsString.trim().split(",");
					
					if(temp.length > 100)
					{
						reporter.incrCounter(LARGE_ITEMSET.UserCounter, 1);
					}

					for (int j = 0; j < temp.length; j++) {

						String item_labelDist = temp[j];
						String[] temp1 = item_labelDist.split("\\:");

						String item = temp1[0].trim();
						double labelDist = Double.parseDouble(temp1[1].trim());

						if (Pinj > 0) {
							/*
							 * We need to emit the true labels here. In this
							 * code we do not capture true labels. So dont use
							 * this until we have a framework to capture true
							 * labels
							 */
							// collector.collect(new Text(user_u), new
							// Text("0,"+item+","+Pinj*labelDist));
						}

						/* Get the neighbors */

						/* Emit the item for each user in the neighborhood */
						for (int i = 0; i < list.size(); i = i + 2) 
						{

							String user_v = list.get(i);
							double weight = Double.parseDouble(list.get(i + 1));

							double score = Pcont * weight * labelDist;

							collector.collect(new Text(user_v), new Text("1,"+ item + "," + score));
							
						}
					}
				} else {
					/*
					 * You are fine, your recommendations after the last
					 * iteration will be emitted by other users
					 */
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void close() {

		}

	}
	
	
		
	
	public static final class LabelsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public static String neighbor_;
		public static double Pinj, Pcont,Pterm;
		public static int keepTopKLabels;
		public static double normalizeSum;

		
		public void configure(JobConf jobConf) {

			neighbor_ = jobConf.get("neighbors");
			
			Pinj = Double.parseDouble(jobConf.get("Pinj"));
			Pcont = Double.parseDouble(jobConf.get("Pcont"));
			Pterm = Double.parseDouble(jobConf.get("Pterm"));

			keepTopKLabels = jobConf.getInt("keepTopKLabels", 50);
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
		        if (x.labelDist < y.labelDist)
		        {
		            return -1;
		        }
		        if (x.labelDist > y.labelDist)
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
			if(queue.size() == keepTopKLabels)
			{
				VertexObject head = queue.peek();
				
				if(obj.labelDist > head.labelDist)
				{
					queue.poll();
					/*If we are removing it, remove from the normalizeSum*/
					normalizeSum=normalizeSum-head.labelDist;
					queue.add(obj);
					/*If we are adding it, we are adding it to the sum*/
					normalizeSum=normalizeSum + obj.labelDist;
				}
			}
			else
			{
				queue.add(obj);
				normalizeSum=normalizeSum + obj.labelDist;
				
			}
		}
		
		/**
		 * <pre>
		 * Input -
		 *    key - value : 
		 *    
		 *    {(v),(0,Yv)} if Pinj > 0
         *    {(v), (1,itemID,Wuv*labelDist)}
         *    
		 * Output - 
		 *    key - v
		 *    value - {itemID:labeldist,...,itemID:labeldist} (Just top 50)
		 * </pre>
		 */

		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashMap<String, Double> injectedDist = new HashMap<String, Double>();
			HashMap<String, Double> weightedNeighDist = new HashMap<String, Double>();
			double normalizationConstant_cont=0;
			double normalizationConstant_inj=0;
			
			String user = key.toString();
			
			Comparator<VertexObject> comparator = new VertexWeightComparator();
			PriorityQueue<VertexObject> queue = new PriorityQueue<VertexObject>(keepTopKLabels, comparator);
			
			while(values.hasNext())
			{
				String value = values.next().toString();
				
				String[] temp = value.split(",");
				
				String itemID = temp[1];
				double labelDist = Double.parseDouble(temp[2]);
				
				if(temp[0].equals("1"))
				{
					/*It is from a neighbor*/
					
					normalizationConstant_cont = normalizationConstant_cont+labelDist;
					
					/*Add it to the hashmap*/
					if(weightedNeighDist.containsKey(itemID))
					{
						double storedValue = weightedNeighDist.get(itemID);
						storedValue = storedValue+labelDist;
						weightedNeighDist.put(itemID, storedValue);
					}
					else
					{
						weightedNeighDist.put(itemID, labelDist);
					}
					
				}
				else
				{
					/*If it is an injected label, then multiply with Pinj and put it in its hashmap*/
				
					normalizationConstant_inj = normalizationConstant_inj+ labelDist;
					
					/*Add it to the hashmap*/
					if(injectedDist.containsKey(itemID))
					{
						double storedValue = injectedDist.get(itemID);
						storedValue = storedValue+labelDist;
						injectedDist.put(itemID, storedValue);
					}
					else
					{
						injectedDist.put(itemID, labelDist);
					}
					
					System.err.println("We did not allow it to use Pinj");
					System.exit(-1);
					
				}
			}
				
			
			/*Once I added everything to the hashmap, just iterate it, normalize it and emit it*/
			
			
			if(Pinj == 0 && injectedDist.size() == 0)
			{
				Iterator<String> it = weightedNeighDist.keySet().iterator();
				
				while(it.hasNext())
				{
					String itemID = it.next();
					
					/*If I am picking top k labels and normalize, I need not normalize now*/
					double normalDist = weightedNeighDist.get(itemID)/normalizationConstant_cont;
					
					VertexObject obj = new VertexObject();
					
					obj.vertexName = itemID;
					obj.labelDist = normalDist;
					

					/*Add it to the queue*/
					addToQueue(queue,obj);
					
//					output.collect(NullWritable.get(), new Text(user+","+itemID+","+normalDist));
				}
			}
			else{
				System.out.println("Either Pinj -ne 0 or size of injected labels is not zero: " + Pinj + "\t" + injectedDist.size());
				System.exit(-1);
			}
			
			/*Collecting the output into a stringbuilder*/
			StringBuilder st = new StringBuilder();
			
			while(queue.size() != 0)
			{
				VertexObject obj = queue.remove();
				st.append(obj.vertexName+":"+(obj.labelDist/normalizeSum)+",");
			}
			
			
			
			/*Now that we are done for this user, reset normalizeSum to 0*/
			
			normalizeSum=0;
			
			
			
			/*Now we have the top k labels. Emit them*/
			
			
			
			/*Add the Dummy label dist. Only emit it if the probability is greater than 0. Also, We should technically add to another
			 * hashmap and normalize all the labels. Because I am sure Pterm is zero, it is going to be ok. But change this for higher
			 * values for Pinj or Pterm*/
			if(Pterm > 0)
			{
				st.append("__DUMMY__:"+Pterm+",");
			}
			
			output.collect(new Text(user), new Text(st.toString()));	
		}
	}	
	
	
	public static final class MyIdentityMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
		/**
		 * Input:
		 *  key - userID
		 *  value - {itemID:labeldist,...,itemID:labeldist} (Just top 50)
		 *  
		 *  Output:
		 *  key - userID
		 *  value - itemID,labelDist
		 */
		 
		public void map(Text key, Text value, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {
			
			
			String[] item_dist = value.toString().split(",");
			
			
			for(String val : item_dist)
			{
//				String[] temp = val.split("\\:");
				
				val = val.replaceAll(":", ",");
				
				collector.collect(key, new Text(val));
			}		
		}
	}
	
	
	public static void main(String[] args) throws Exception
	{
		
		if(args.length < 7)
		{
			System.out.println("USAGE: \n" +
					"\t\tThe first parameter the path to label distribution\n" +
					"\t\tThe second parameter should be path to user neighbor folder\n" +
					"\t\tThe third parameter should be path to base output folder for edge creation\n" +
					"\t\tThe fourth parameter is the number of labels to use\n 50" +
					"\t\tThe fifth parameter is the number of iterations to run the algorithm\n 10" +
					"\t\tThe sixth parameter is the number of reduce tasks to be used\n 10" +
					"\t\tThe seventh parameter is the paramString 1\n");
			System.exit(0);
		}
		
		System.out.println("\n*****Loading the Configuration for AdsorptionJob (modified)*****\n");
		configure(args);
		
		loadcvParams();
		
		loadProbabilities();
		
		LinkedList<Double> probs = probabilities.get(paramString_);
		
		inj = probs.get(0);
		cont = probs.get(1);
		term = probs.get(2);
		
		System.out.println("Inj: "+ inj + " cont: " + cont + " term: " +term);
		
		
		System.out.println("\n*****Done....*****\n");
		
		/*Start the iterations here*/
		
		int iteration=0;
		
		System.out.println("Running the data preparation job");
		JobConf conf = new JobConf(Parallel_2.class);
		createJob0(conf);
		JobClient.runJob(conf);
		
		while (iteration<numIterations_){
			
//			if(inj > 0)
//			{
//				System.out.println("Calling the injection label distribution");
//				JobClient.runJob(createJob1(iteration));
//			}
//			
			System.out.println("\nComputing all\n");
			JobClient.runJob(createJob1(iteration));
			
			iteration++;
			
//			printVertices_Edges();
			
		}
		
		System.out.println("Running the output preparation job");
		
		JobClient.runJob(createJob2(iteration-1));
		
	}
}
