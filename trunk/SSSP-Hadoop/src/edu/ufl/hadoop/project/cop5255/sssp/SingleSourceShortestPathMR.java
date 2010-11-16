/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.sssp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.ufl.hadoop.project.cop5255.util.Edge;
import edu.ufl.hadoop.project.cop5255.util.GraphNode;
import edu.ufl.hadoop.project.cop5255.util.NodeColor;

/**
 * @author Owner
 * 
 */
public class SingleSourceShortestPathMR extends Configured implements Tool {

	public static final Log LOG = LogFactory
			.getLog("edu.ufl.hadoop.project.cop5255.sssp.SingleSourceShortestPathMR");

	public static class MapClass extends
			Mapper<LongWritable, Text, IntWritable, GraphNode> {

		public void map(LongWritable key, Object value, OutputCollector<IntWritable, Object> output, Reporter reporter) throws IOException {

			GraphNode node = (GraphNode)value;
			LOG.info("Map executing for key [" + key.toString()
					+ "] and value [" + value.toString() + "]");

			// For each GRAY node, emit each of the edges as a new node (also
			// GRAY)
			if (node.getNodeColor().equals(NodeColor.GRAY)) {
				for (Edge edge : node.getEdges()) {
					GraphNode newNode = new GraphNode(edge.getToNode().getId(),
							NodeColor.GRAY, node.getWeight()
									.getWeightInDouble() + 1/*
															 * edge.getWeightInDouble
															 * ()
															 */);
					// blank edges
					output.collect(new IntWritable(newNode.getId()), newNode);
				}
				// We're done with this node now, color it BLACK
				node.updateColor(NodeColor.BLACK);
			}

			// No matter what, we emit the input node
			// If the node came into this method GRAY, it will be output as
			// BLACK
			output.collect(new IntWritable(node.getId()), node);

			LOG.info("Map outputting for key[" + node.getId() + "] and value ["
					+ node.toString() + "]");

		}
	}

	public static class Reduce extends
			Reducer<IntWritable, GraphNode, IntWritable, Text> {

		/**
		 * Make a new node which combines all information for this single node
		 * id. The new node should have - The full list of edges - The minimum
		 * distance - The darkest Color
		 */
		public void reduce(IntWritable key, Iterator<Object> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			LOG.info("Reduce executing for input key [" + key.toString() + "]");

			List<Edge> edges = new ArrayList<Edge>();
			double distance = Double.POSITIVE_INFINITY;
			NodeColor color = NodeColor.WHITE;

			while (values.hasNext()) {
				GraphNode node = (GraphNode)values.next();

				// GraphNode u = new GraphN(key.get() + "\t" +
				// value.toString());

				// One (and only one) copy of the node will be the fully
				// expanded
				// version, which includes the edges
				if (node.getEdges().size() > 0) {
					edges.addAll(node.getEdges());
				}

				// Save the minimum distance
				if (node.getWeight().getWeightInDouble() < distance) {
					distance = node.getWeight().getWeightInDouble();
				}

				// Save the darkest color
				if (node.getNodeColor().getColorIndex() > color.getColorIndex()) {
					color = node.getNodeColor();
				}

			}

			GraphNode n = new GraphNode(key.get(), color, distance);
			n.addEdges(edges);
			output.collect(key, new Text(n.getId() + " - " + distance));
			LOG.info("Reduce outputting final key [" + key + "] and value ["
					+ n + "]");
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("Started execution for process Single Source Shortest Path");
		int iterationCount = 0;

		while (iterationCount < 4) {

			String input;
			if (iterationCount == 0)
				input = "input-graph";
			else
				input = "output-graph-" + iterationCount;

			String output = "output-graph-" + (iterationCount + 1);

			Configuration conf = new Configuration();
			Job job = new Job(conf);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			iterationCount++;
		}

		return 0;
	}

}
