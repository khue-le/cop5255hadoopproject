package edu.ufl.hadoop.project.cop5255.builder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.ufl.hadoop.project.cop5255.util.Edge;

public class GraphBuilder {

  private static int NO_OF_EDGES = 0;
	
  public static class GraphNodeBuilderMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	int counter = 0;
    	while(counter < NO_OF_EDGES) {
    		Edge edge = EdgeBuilder.build(NO_OF_EDGES);
    		context.write(new LongWritable(Long.valueOf(edge.getToNode().getId())), new Text("," + edge + "(" + edge.getWeightInDouble() + "),"));
    	}
    }
  }
  
  public static class GraphBuilderReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	  
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
      StringBuilder finalEdgeList = new StringBuilder();
      for (Text val : values) {
    	  finalEdgeList.append(val);
      }
      context.write(key, new Text(finalEdgeList.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Random Graph Builder");
    job.setJarByClass(GraphBuilder.class);
    job.setMapperClass(GraphNodeBuilderMapper.class);
    job.setCombinerClass(GraphBuilderReducer.class);
    job.setReducerClass(GraphBuilderReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    NO_OF_EDGES = Integer.valueOf(args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
