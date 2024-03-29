package edu.ufl.hadoop.project.cop5255.builder;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.ufl.hadoop.project.cop5255.filter.InputPathFilter;
import edu.ufl.hadoop.project.cop5255.sssp.Node;
import edu.ufl.hadoop.project.cop5255.util.Edge;

import edu.ufl.hadoop.project.cop5255.util.NodeColor;
import edu.ufl.hadoop.project.cop5255.util.Weight;

public class GraphBuilder {

	public static final Log LOG = LogFactory
			.getLog("edu.ufl.hadoop.project.cop5255.builder.GraphBuilder");

	public static class GraphNodeBuilderMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			long counter = 0;
			String contents[] = value.toString().split("\t");
			Long totalNoOfEdges = Long.valueOf(contents[0]);
			Long startNode = Long.valueOf(contents[1]);
			while (counter < totalNoOfEdges) {
				Edge edge = EdgeBuilder.build(totalNoOfEdges);
				if (counter == 0) {
					edge = new Edge(new Node(startNode, NodeColor.GRAY,
							Weight.ZERO.getWeightInDouble()), value.toString().equals(
							edge.getToNode()) ? edge.getFromNode() : edge
							.getToNode(), edge.getWeightInDouble());
				}
				System.out.println("Generated Map for "
						+ Long.valueOf(edge.getToNode().getId())
						+ " - "
						+ new Text("," + edge.getFromNode().getId() + "("
								+ edge.getWeightInDouble() + "),"));
				LOG.info("Generated Map for "
						+ Long.valueOf(edge.getToNode().getId())
						+ " - "
						+ new Text(edge.getFromNode().getId() + "("
								+ edge.getWeightInDouble() + "),"));
				StringBuilder newKey = new StringBuilder(String.valueOf(edge
						.getToNode().getId()));
				if (startNode == edge.getToNode().getId()) {
					newKey.append("S");
				}
				context.write(new Text(newKey.toString() /*
														 * + startNode ==
														 * edge.getToNode
														 * ().getId() ? "S" : ""
														 */), new Text(edge
						.getFromNode().getId()
						+ "(" + edge.getWeightInDouble() + "),"));
				LOG.info("Generated Map for "
						+ Long.valueOf(edge.getFromNode().getId())
						+ " - "
						+ new Text(edge.getToNode().getId() + "("
								+ edge.getWeightInDouble() + "),"));
				System.out.println("Generated Map for "
						+ Long.valueOf(edge.getFromNode().getId())
						+ " - "
						+ new Text(edge.getToNode().getId() + "("
								+ edge.getWeightInDouble() + "),"));
				newKey = new StringBuilder(String.valueOf(edge.getFromNode()
						.getId()));
				if (startNode == edge.getFromNode().getId()) {
					newKey.append("S");
				}
				context.write(new Text(newKey.toString()/*
														 * + startNode ==
														 * edge.getFromNode
														 * ().getId() ? "S" : ""
														 */), new Text(edge
						.getToNode().getId()
						+ "(" + edge.getWeightInDouble() + "),"));
				counter++;
			}
		}
	}

	public static class GraphBuilderReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder finalEdgeList = new StringBuilder();
			for (Text val : values) {
				if (!finalEdgeList.toString().contains(
						val.toString().substring(0,
								val.toString().indexOf("(") + 1))) {
					finalEdgeList.append(val);
				}
			}
			LOG.info("Finally writing " + finalEdgeList.toString());
			int startNodeIndex = key.toString().indexOf("S");
			if (startNodeIndex != -1) {
				context.write(new Text(key.toString().substring(0,
						startNodeIndex)), new Text("\t"
						+ finalEdgeList.toString().substring(0,
								finalEdgeList.length() - 1) + "|0|GRAY"));
			} else {
				context.write(key, new Text("\t"
						+ finalEdgeList.toString().substring(0,
								finalEdgeList.length() - 1)
						+ "|Integer.MAX_VALUE|WHITE"));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		if (inputPath.getFileSystem(conf).getFileStatus(inputPath).isDir()) {
			System.out.println("Error : Input path should be a File, not a directory");
			return;
		} else {
			Job job = new Job(conf, "Random Graph Builder");
			job.setJarByClass(GraphBuilder.class);
			job.setMapperClass(GraphNodeBuilderMapper.class);
			job.setReducerClass(GraphBuilderReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPathFilter(job, InputPathFilter.class);
			inputPath.getFileSystem(conf).delete(outputPath, true);
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
