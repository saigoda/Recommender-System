package heterogeneous.brainStorm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class to compute the conditional probability between 2 networks in a serial way. 
 * 
 * In this class, we will permute through the number of networks selecting 2 at each time and compute the probability:
 * 
 * If the total number of networks are N=3 then we compute 6, if N=4 then 12 ...
 * 
 * @author rohitp
 *
 */
public class ComputeConditionalProbability {

	
	public static HashMap<String,Double> intersectionCounts = new HashMap<String,Double>();
	public static HashMap<String,Double> linkCounts = new HashMap<String,Double>();
//	public static HashMap<String,Double> conditionalProbability = new HashMap<String,Double>();
	
	public static String baseIntersectionPath;
	public static String baseLinkCountPath;
	public static String conditionalProbabilityPath;
	public static int numNetworks;
	
	
	public static void ComputeProbability(String baseIntersectionPath, String baseLinkCountPath, String conditionalProbabilityPath, int numNetworks) {
		try {

			FileSystem fs = FileSystem.get(new Configuration());
			
			for(int i=1;i<=numNetworks;i++)
			{
				/*Get the link information for this network*/
				BufferedReader br = null;
				
				try
				{
					String path = baseLinkCountPath+i+"/part-00000";
					
//					System.out.println(path);
					
					br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
					
					String line ="";
					
					while((line = br.readLine()) !=null)
					{
//						System.out.println(line);
						linkCounts.put(i+"", Double.parseDouble(line.trim()));
						
						
					}
					
					br.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				
				for(int j=i+1;j<=numNetworks;j++)
				{
					/*Get the intersection count*/
					try
					{
						String path = baseIntersectionPath+i+"_"+j+"/part-00000";
//						System.out.println(path);
						
						br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
						
						String line ="";
						
						while((line = br.readLine()) !=null)
						{
//							System.out.println(line);
							intersectionCounts.put(i+"_"+j, Double.parseDouble(line.trim()));
							
						}
						
						br.close();
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
				}
			}
			
			/*Not that we have stored intersection counts and the link counts for the networks, here we compute the conditional probability*/
			
			System.out.println("Size of linkCounts is: " + linkCounts.size());
			System.out.println("Size of intersectionCounts is: " + intersectionCounts.size());
			
			
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(conditionalProbabilityPath+"part-00000"),true)));
			
			for(int i=1;i<=numNetworks;i++)
			{
				for(int j=1;j<=numNetworks;j++)
				{
					if(i == j)
						continue;
					
					double intersectionCount = -1;
					double linkCount = -1;
					
					if(i < j)
					{
						/*This is because the intersection count is same irrespective of the order of the networks*/
						String key = i+"_"+j;
						
						if(intersectionCounts.containsKey(key))
						{
							intersectionCount = intersectionCounts.get(key);
						}
						else
						{
							System.err.println("Intersection count not found for key:" + key);
							System.exit(1);
						}
					}
					else
					{
						String key = j+"_"+i;
						
						if(intersectionCounts.containsKey(key))
						{
							intersectionCount = intersectionCounts.get(key);
						}
						else
						{
							System.err.println("Intersection count not found for key:" + key);
							System.exit(1);
						}
					}
					
					linkCount = linkCounts.get(j+"");
					
					if(intersectionCount != -1 && linkCount !=-1)
					{
						/*Everything is correct, compute p(i|j)*/
						
						double conditionalProb = intersectionCount/linkCount;
						
						String key = i+"_"+j;
						
						bw.write(key+"\t"+conditionalProb+"\n");
					}
					else
					{
						System.err.print("Something went wrong: the counts are still -1");
						System.exit(1);
					}
				}
			}
			
			bw.flush();
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	
	public static void main(String[] args)
	{
		if(args.length != 4)
		{
			System.exit(1);
		}
		
		System.out.println("Starting the serial code to compute the conditional probability");
		ComputeProbability(args[0], args[1], args[2], Integer.parseInt(args[3]));
		
		
	}
	
	
}
