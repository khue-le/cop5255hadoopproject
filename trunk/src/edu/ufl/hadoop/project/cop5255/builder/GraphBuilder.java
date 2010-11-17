package edu.ufl.hadoop.project.cop5255.builder;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.ufl.hadoop.project.cop5255.util.Edge;

public class GraphBuilder {

	public static final Log LOG = LogFactory
	.getLog("edu.ufl.hadoop.project.cop5255.builder.GraphBuilder");

  private static int NO_OF_EDGES = 0;
	
  public static class GraphNodeBuilderMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	int counter = 0;
    	while(counter < 6000000) {
    		Edge edge = EdgeBuilder.build(6000000);
    		System.out.println("Generated Map for " + Long.valueOf(edge.getToNode().getId()) + " - " + new Text("," + edge.getFromNode().getId() + "(" + edge.getWeightInDouble() + "),"));
    		LOG.info("Generated Map for " + Long.valueOf(edge.getToNode().getId()) + " - " + new Text(edge.getFromNode().getId() + "(" + edge.getWeightInDouble() + "),"));
    		context.write(new LongWritable(Long.valueOf(edge.getToNode().getId())), new Text(edge.getFromNode().getId() + "(" + edge.getWeightInDouble() + "),"));
    		LOG.info("Generated Map for " + Long.valueOf(edge.getFromNode().getId()) + " - " + new Text(edge.getToNode().getId() + "(" + edge.getWeightInDouble() + "),"));
    		System.out.println("Generated Map for " + Long.valueOf(edge.getFromNode().getId()) + " - " + new Text(edge.getToNode().getId() + "(" + edge.getWeightInDouble() + "),"));
    		context.write(new LongWritable(Long.valueOf(edge.getFromNode().getId())), new Text(edge.getToNode().getId() + "(" + edge.getWeightInDouble() + "),"));
    		counter++;
    	}
    }
  }
  
  public static class GraphBuilderReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	  
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
      StringBuilder finalEdgeList = new StringBuilder();
      for (Text val : values) {
    	  finalEdgeList.append(val);
      }
		LOG.info("Finally writing " + finalEdgeList.toString());
      context.write(key, new Text("\t" + finalEdgeList.toString() + (key.equals(new LongWritable(1)) ? "|0" : "|INFINITY") + (key.equals(new LongWritable(1)) ? "|GRAY" : "|WHITE")));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Random Graph Builder");
    job.setJarByClass(GraphBuilder.class);
    job.setMapperClass(GraphNodeBuilderMapper.class);
//    job.setCombinerClass(GraphBuilderReducer.class);
    job.setReducerClass(GraphBuilderReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    NO_OF_EDGES = Integer.valueOf(args[0]);
    FileInputFormat.setInputPaths(job, "/docs");
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
