package CreateGraph;

import java.io.IOException;
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

public class CombinedWeightsOfNetworks extends Configured implements Tool {

    public static class CombinedWeightsOfNetworksMap extends MapReduceBase
            implements Mapper<Text, Text, Text, Text>{

        @Override
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         */
        public void map(Text key, Text value,
                OutputCollector<Text, Text> output, Reporter arg3)
                throws IOException {
            String user = key.toString().split(":")[0];
            String userDegree = key.toString().split(":")[1];
            String[] neighbors = value.toString().split(",");
            for (String neighbor : neighbors) {
                output.collect(new Text(user + "," + neighbor.split(":")[0]),
                        new Text(userDegree + ":" + neighbor));
            }

        }

    }

    public static class CombinedWeightsOfNetworksReduce extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String[] keySplits = key.toString().split(",");
            int userDegree = 0;
            String newNeighbor = "";
            String newNeighborKey = "";
            int totalItemCount = 0;
            int commonItemCount = 0;
            while (values.hasNext()) {
                String neighbor = values.next().toString();
                String[] neighborSplits = neighbor.split(":");
                userDegree += Integer.parseInt(neighborSplits[0]);
                if (newNeighborKey.length() == 0)
                    newNeighborKey = neighborSplits[1];
                totalItemCount += Integer.parseInt(neighborSplits[2]);
                commonItemCount += Integer.parseInt(neighborSplits[3]);
            }
            newNeighbor = newNeighborKey + ":" + totalItemCount + ":"
                    + commonItemCount;

            output.collect(new Text(keySplits + ":" + userDegree), new Text(
                    newNeighbor));
        }
    }
    
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new CombinedWeightsOfNetworks(), args);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
            JobConf job = new JobConf(super.getConf(), this.getClass());
            job.setJarByClass(this.getClass());
            job.setJobName("Combine weights of n networks");
            job.setJobPriority(JobPriority.VERY_HIGH);
            job.setMapperClass(CombinedWeightsOfNetworksMap.class);
            job.setReducerClass(CombinedWeightsOfNetworksReduce.class);
            job.setNumReduceTasks(30);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            for(int i=0;i<args.length-2;i++)
                MultipleInputs.addInputPath(job, new Path(args[i]), KeyValueTextInputFormat.class);
            FileSystem.get(job).delete(new Path(args[args.length-1]));
            FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
            JobClient.runJob(job);
            System.out.println("***********DONE********");
            return 0;
    }
}
