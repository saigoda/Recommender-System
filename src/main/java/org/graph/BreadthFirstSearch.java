package org.graph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.graph.Node;

public class BreadthFirstSearch extends Configured implements Tool {

    /**
     * @param args
     */

    /*
     * 
     * The mapper takes as the input the modified neighborhood and executes the
     * BFS algorithm
     * 
     * The mapper input is of the format
     * 
     * <KEY>\t<neighbor1>,<neighbor2>,<neighbor3>,<neighbor1>,|distance|Color|Parent
     * 
     * Mapper output
     * 
     * Output Key: <KEY> Output Value:
     * <neighbor1>,<neighbor2>,<neighbor3>,<neighbor1
     * >,|distance|Color|Parent|Score
     * 
     * Supporting class used for this class is Node.java(org.graph.Node)
     */
    public static class BreadthFirstSearchMap extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        /*
         * 
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         * 
         * Input Example: 1000_user:30
         * 1_user:10:10,4455_user:30:9,3_user:45:7,|0|GRAY|source|1 Output
         * Key--1000_user:30
         * Value--1_user:10:10,4455_user:30:9,3_user:45:7,|0|GRAY|source|1
         */
        public void map(LongWritable arg0, Text arg1,
                OutputCollector<Text, Text> output, Reporter arg3)
                throws IOException {
            // TODO Auto-generated method stub
            // The Node file takes a neighborhood as input and stores the
            // parent,distance,color and edges of the nodes as it's properties.
            Node inputNode = new Node(arg1.toString().trim());
            System.out.println("Printing nodeInfo" + inputNode.getNodeInfo());
            if (inputNode.getColor().equals(Node.Color.GRAY.name())) {// All the
                                                                      // nodes
                                                                      // which
                // are coloured gray
                // are processed
                // first. The
                // process continues
                // until no new
                // states can be
                // reached or all
                // gray nodes are
                // done
                for (String edges : inputNode.getEdges()) {
                    Node adjacentNode = new Node();
                    String[] splits = edges.split(":");
                    edges = splits[0] + ":" + splits[1];
                    adjacentNode.setId(edges);// All gray node edges are colored
                                              // GRAY so that they are
                                              // processed next. The current
                                              // GRAY node is colored BLACK
                    adjacentNode.setColor(Node.Color.GRAY.name());
                    // Multiple the score of the parent with the distribution of
                    // weight to get the score of this node
                    adjacentNode.setScore((Double.parseDouble(inputNode
                            .getScore()) * (Double.parseDouble(splits[2])))
                            + "");
                    adjacentNode.setDistance(inputNode.getDistance() + 1);
                    adjacentNode.setParent(inputNode.getId());// The parent of
                                                              // the edges are
                                                              // set to the
                                                              // current GRAY
                                                              // node
                    System.out.println(adjacentNode.getId());
                    output.collect(new Text(adjacentNode.getId()), new Text(
                            adjacentNode.getNodeInfo()));
                }// The edge is emitted along with all it's information.
                 // Notice that for the adjacent node we are not setting it's
                 // edges. All other properties are set
                inputNode.setColor(Node.Color.BLACK.name());
            }
            output.collect(new Text(inputNode.getId()),
                    new Text(inputNode.getNodeInfo()));
        }

    }

    static enum MoreIterations {
        numberOfIterations
    }

    public static class BreadthFirstSearchReduce extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {

        @Override
        /*
         * 
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
         * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         * 
         * Possible input to reducer
         * 
         * 1_user:10 ,|1|GRAY|1000_user:30 1_user:10
         * 123_user:30,345_user:10,|Integer.MAX_VALUE|WHITE|null|<score>
         * 
         * Output:
         * 
         * 1_user:10 123_user:30,345_user:10,|1|GRAY|1000_user:30|0
         */
        public void reduce(Text key, Iterator<Text> arg1,
                OutputCollector<Text, Text> output, Reporter arg3)
                throws IOException {
            // TODO Auto-generated method stub
            Node outNode = new Node();// All the possible inputs are taken and
                                      // the node's
                                      // edges,parent,distance,color are
                                      // updated
            outNode.setId(key.toString());
            while (arg1.hasNext()) {
                Text value = arg1.next();
                Node inNode = new Node(key.toString() + "\t" + value.toString());
                if (inNode.getEdges().size() > 0) {
                    outNode.setEdges(inNode.getEdges());
                }
                if (Node.Color.valueOf(inNode.getColor()).compareTo(
                        Node.Color.valueOf(outNode.getColor())) > 0) {
                    outNode.setColor(inNode.getColor());
                }
                if (Integer.parseInt(inNode.getDistance()) < Integer
                        .parseInt(outNode.getDistance())) {
                    outNode.setDistance(inNode.getDistance());
                    // if the distance gets updated then the predecessor node
                    // that was responsible for this distance will be the parent
                    // node
                    outNode.setParent(inNode.getParent());
                }
            }
            output.collect(key, new Text(outNode.getNodeInfo()));
            if (outNode.getColor().equals(Node.Color.GRAY))
                arg3.getCounter(MoreIterations.numberOfIterations)
                        .increment(1L);// If the node's color is GRAY then there
                                       // are nodes in the search frontier to
                                       // be expanded and hence the increment
                                       // value is increased
        }

    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BreadthFirstSearch(),
                args);
        if (args.length != 2) {
            System.err.println("Usage: <in> <output name> ");
        }
        System.exit(res);
    }

    @SuppressWarnings("deprecation")
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int iterationCount = 0; // counter to set the ordinal number of the
                                // intermediate outputs
        long terminationValue = 1;
        while (terminationValue > 0) {
            JobConf job = new JobConf(this.getConf(), this.getClass());
            job.setJarByClass(this.getClass());
            job.setJobName("Breadth First Search");
            job.setJobPriority(JobPriority.VERY_HIGH);
            job.setMapperClass(BreadthFirstSearchMap.class);
            job.setReducerClass(BreadthFirstSearchReduce.class);
            job.setNumReduceTasks(40);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            String input, output;// For the first iteration the input is the
                                 // input given by the user. For the other
                                 // iterations the input is the previous
                                 // output
            if (iterationCount == 0) {
                input = args[0];
            } else {
                input = args[1] + "/" + iterationCount;
            }
            output = args[1] + "/" + (iterationCount + 1);
            FileInputFormat.setInputPaths(job, input);
            FileSystem.get(job).delete(new Path(output));
            FileOutputFormat.setOutputPath(job, new Path(output));
            RunningJob runningJob = JobClient.runJob(job);
            terminationValue = runningJob.getCounters().getCounter(
                    MoreIterations.numberOfIterations);// In the reducer we
                                                       // increment this value
                                                       // if there are GRAY
                                                       // nodes in the search
                                                       // frontier. If there
                                                       // are no nodes then
                                                       // this value is 0 and
                                                       // loop terminates
            System.out.println("Termination value is:" + terminationValue);
            iterationCount++;
            System.out.println("***********DONE********");
        }
        return 0;
    }

}
