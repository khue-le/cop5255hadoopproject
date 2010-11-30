package edu.ufl.hadoop.project.cop5255.sssp;

import java.io.IOException;
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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ufl.hadoop.project.cop5255.util.Edge;
import edu.ufl.hadoop.project.cop5255.util.NodeColor;

/**
 * This is an example Hadoop Map/Reduce application.
 * 
 * It inputs a map in adjacency list format, and performs a breadth-first
 * search. The input format is ID EDGES|DISTANCE|COLOR where ID = the unique
 * identifier for a node (assumed to be an int here) EDGES = the list of edges
 * emanating from the node (e.g. 3,8,9,12) DISTANCE = the to be determined
 * distance of the node from the source COLOR = a simple status tracking field
 * to keep track of when we're finished with a node It assumes that the source
 * node (the node from which to start the search) has been marked with distance
 * 0 and color GRAY in the original input. All other nodes will have input
 * distance Integer.MAX_VALUE and color WHITE.
 */
public class SingleSourceShortestPathMR extends Configured implements Tool {

	public static final Log LOG = LogFactory
			.getLog("org.apache.hadoop.examples.GraphSearch");

	/**
	 * Nodes that are Color.WHITE or Color.BLACK are emitted, as is. For every
	 * edge of a Color.GRAY node, we emit a new Node with distance incremented
	 * by one. The Color.GRAY node is then colored black and is also emitted.
	 */
	public static class MapClass extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			LOG.info("Map executing for key [" + key.toString()
					+ "] and value [" + value.toString() + "]");

			Node node = new Node(value.toString());

			// For each GRAY node, emit each of the edges as a new node (also
			// GRAY)
			if (node.getColor() == NodeColor.GRAY) {
				for (Edge v : node.getEdges()) {
					Node vnode = v.getToNode();
					vnode.setDistance(node.getDistance() + vnode.getDistance());
					vnode.setColor(NodeColor.GRAY);
					output.collect(new LongWritable(vnode.getId()), vnode
							.getLine());
				}
				// We're done with this node now, color it BLACK
				node.setColor(NodeColor.BLACK);
			}

			// No matter what, we emit the input node
			// If the node came into this method GRAY, it will be output as
			// BLACK
			output.collect(new LongWritable(node.getId()), node.getLine());

			LOG.info("Map outputting for key[" + node.getId() + "] and value ["
					+ node.getLine() + "]");
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		/**
		 * Make a new node which combines all information for this single node
		 * id. The new node should have - The full list of edges - The minimum
		 * distance - The darkest Color
		 */
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			LOG.info("Reduce executing for input key [" + key.toString() + "]");

			List<Edge> edges = null;
			Double distance = Double.MAX_VALUE;
			NodeColor color = NodeColor.WHITE;

			while (values.hasNext()) {
				Text value = values.next();

				Node u = new Node(key.get() + "\t" + value.toString());

				// One (and only one) copy of the node will be the fully
				// expanded
				// version, which includes the edges
				if (u.getEdges().size() > 0) {
					edges = u.getEdges();
				}

				// Save the minimum distance
				if (u.getDistance() < distance) {
					distance = u.getDistance();
				}

				// Save the darkest color
				if (u.getColor().getColorIndex() > color.getColorIndex()) {
					color = u.getColor();
				}

			}

			Node n = new Node(key.get());
			n.setDistance(distance);
			n.setEdges(edges);
			n.setColor(color);
			output.collect(key, new Text(n.getLine()));
			LOG.info("Reduce outputting final key [" + key + "] and value ["
					+ n.getLine() + "]");
		}
	}

	static int printUsage() {
		System.out
				.println("graphsearch [-m <num mappers>] [-r <num reducers>]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for word count map/reduce program. Invoke this method to
	 * submit the map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {

		int iterationCount = 0;
		Configuration conf = new Configuration();
		while (keepGoing(iterationCount)) {

			String input;
			if (iterationCount == 0)
				input = "inputgraph";
			else
				input = "output-graph-" + iterationCount;

			String output = "output-graph-" + (iterationCount + 1);

			Path deletePath = new Path("output-graph-" + (iterationCount - 1));
			Job job = new Job(conf, "Single Source Shortest Path");
			// the keys are the unique identifiers for a Node (ints in this
			// case).
			job.setOutputKeyClass(IntWritable.class);
			// the values are the string representation of a Node
			job.setOutputValueClass(Text.class);

			job.setMapperClass(MapClass.class);
			job.setReducerClass(Reduce.class);

			for (int i = 0; i < args.length; ++i) {
				if ("-m".equals(args[i])) {
					// job.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					job.setNumReduceTasks(Integer.parseInt(args[++i]));
				}
			}

			if (deletePath.getFileSystem(conf).exists(deletePath))
				deletePath.getFileSystem(conf).delete(deletePath, true);

			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			iterationCount++;
		}

		return 0;
	}

	private boolean keepGoing(int iterationCount) {
		if (iterationCount >= 4) {
			return false;
		}

		return true;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SingleSourceShortestPathMR(), args);
		System.exit(res);
	}

}
