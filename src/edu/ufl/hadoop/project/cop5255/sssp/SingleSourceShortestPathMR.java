package edu.ufl.hadoop.project.cop5255.sssp;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ufl.hadoop.project.cop5255.util.Edge;
import edu.ufl.hadoop.project.cop5255.util.NodeColor;

/**
 * This is an example Hadoop Map/Reduce application.
 * 
 * It inputs a map in dynamic adjacency list format. The input format is ID EDGES(WEIGHT)|DISTANCE|COLOR where ID = the 
 * unique key value for a node EDGES = the  comma delimited list of edges  emanating from the node (e.g. 3(40.0)
 * ,8(1.0),9(6.0),12(18.0),the edges have corresponding  associative weights, DISTANCE = the to be 
 * determined  from the source COLOR = a simple status tracking field to keep track of when we're finished with a node
 *  It assumes that the source  node (the node from which to start the search) has been marked with distance
 * 0 and color GRAY in the original input. All other nodes will have input
 * distance Integer.MAX_VALUE and color WHITE.
 *
 *For example consider the input 
 * 1		2(2.0),3(5.0),4(2.0)|0|GRAY
 * 2        1(2.0),4(4.0)|Integer.MAX_VALUE|WHITE    
 * 3        1(5.0),4(1.0)|Integer.MAX_VALUE|WHITE 
 * 4        1(2.0),2(4.0),3(1.0)|Integer.MAX_VALUE|WHITE
 * 
 * suppose we start from the input graph with node 1 as the source node.As this is the special node with key value 0 and  node color gray
 * the Mapper  will emit a  new gray node with distance =distance +weight of the node.
 * 
 * 1       2(2.0),3(5.0),4(2.0)|0|BLACK
 * 2       NULL|2.0|GRAY|
 * 3       NULL|5.0|GRAY|
 * 4       NULL|5.0|GRAY|
 * 2       1(2.0),4(4.0)|Integer.MAX_VALUE|WHITE    
 * 3       1(5.0),4(1.0)|Integer.MAX_VALUE|WHITE 
 * 4       1(2.0),2(4.0),3(1.0)|Integer.MAX_VALUE|WHITE 
 * 
 *  Above  when the Mappers "explodes " a gray nodes and create corresponding  nodes for new edges , since they y do not know what to write for the edges 
 *  they mark them blank. 
 *   
 * 2       NULL|2.0|GRAY|
 * 2       1(2.0),4(4.0)|Integer.MAX_VALUE|WHITE
 *  
 * 3       NULL|5.0|GRAY|   
 * 3       1(5.0),4(1.0)|Integer.MAX_VALUE|WHITE 
 * 
 * 4       1(2.0),2(4.0),3(1.0)|Integer.MAX_VALUE|WHITE 
 * 4       NULL|5.0|GRAY|
 * 
 *  Reduce job is to take the above data and reduce it to.
 *
 * 2       1(2.0),4(4.0)|2.0|GRAY
 * 3       1(5.0),4(1.0)|5.0|GRAY 
 * 4       1(2.0),2(4.0),3(1.0)|5.0|GRAY 
 *  subsequent iteration will continue in the same way until there
 *  is at least one gray node.
 * 
*/
public class SingleSourceShortestPathMR extends Configured implements Tool {

	public static final Log LOG = LogFactory
			.getLog("org.apache.hadoop.examples.GraphSearch");

	/**
	 * Nodes having there  Color WHITE or BLACK are emitted, as is. For every
	 * edge of a GRAY Color node, we emit a new Node with distance incremented
	 * by one. The GRAY Color  node is then colored black and is also emitted.
	 */
	public static class MapClass extends MapReduceBase implements
    Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {

			LOG.info("Map executing for key [" + key.toString()
					+ "] and value [" + value.toString() + "]");

			Node node = new Node(value.toString());
			if (node.getColor() == NodeColor.GRAY) {
				for (Edge v : node.getEdges()) {
					Node vnode = v.getToNode();
					vnode.setDistance(node.getDistance() + vnode.getDistance());
					vnode.setColor(NodeColor.GRAY);
					output.collect(new LongWritable(vnode.getId()), vnode
							.getLine());
				}
			
				node.setColor(NodeColor.BLACK);
			}
			output.collect(new LongWritable(node.getId()), node.getLine());

			LOG.info("Map outputting for key[" + node.getId() + "] and value ["
					+ node.getLine() + "]");
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends MapReduceBase implements
    Reducer<LongWritable, Text, LongWritable, Text> {

		/**
		 * Make an instance of new  node which combines all information for this single node
		 * It  should have - The full list of edges -  minimum distance among all  value   -  darkest Color
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

				System.out.println("In Reduce  - " + value.toString());
				Node u = new Node(key.get() + "\t" + value.toString());

				if (u.getEdges().size() > 0) {
					edges = u.getEdges();
				}

				// update distance to the distance
				if (u.getDistance() < distance) {
					distance = u.getDistance();
				}

				// update color to the darlkest color
				if (u.getColor().getColorIndex() > color.getColorIndex()) {
					color = u.getColor();
				}

			}

			Node n = new Node(key.get());
			n.setEdges(edges);
			n.setDistance(distance);
			n.setColor(color);
			output.collect(key, new Text(n.getLine()));
			LOG.info("Reduce outputting final key [" + key + "] and value ["
					+ n.getLine() + "]");
		}
	}

	/** 
	 * 
	 * get the  hadoop job configuration
	 */
	private JobConf getJobConf(String[] args) {
		    JobConf conf = new JobConf(getConf(), SingleSourceShortestPathMR.class);
		    conf.setJobName("graphsearch");

		    // Node key value
		    conf.setOutputKeyClass(LongWritable.class);
		    // the values are the string representation of a Node adjacency list.   
		    conf.setOutputValueClass(Text.class);

		    conf.setMapperClass(MapClass.class);
		    conf.setReducerClass(Reduce.class);

		    for (int i = 0; i < args.length; ++i) {
		      if ("-m".equals(args[i])) {
		        conf.setNumMapTasks(Integer.parseInt(args[++i]));
		      } else if ("-r".equals(args[i])) {
		        conf.setNumReduceTasks(Integer.parseInt(args[++i]));
		      }
		    }

		    LOG.info("The number of reduce tasks has been set to " + conf.getNumReduceTasks());
		    LOG.info("The number of mapper tasks has been set to " + conf.getNumMapTasks());

		    return conf;
		  }


	/**
	 * The main driver for word count map/reduce program. Invoke this method to
	 * submit the map/reduce job.
	 * 
	 * @throws IOException
	  */
	public int run(String[] args) throws Exception {

		int iterationCount = 0;
		while (isIterationOver(iterationCount, Long.valueOf(args[2]))) {

			String input;
			if (iterationCount == 0)
				input = args[0];
			else
				input = args[1] + iterationCount;

			String output = args[1] + (iterationCount + 1);

			Path deletePath = new Path(args[1] + (iterationCount - 1));
		      JobConf conf = getJobConf(args);
		      FileInputFormat.setInputPaths(conf, new Path(input));
		      FileOutputFormat.setOutputPath(conf, new Path(output));
		      RunningJob job = JobClient.runJob(conf);


			if (deletePath.getFileSystem(conf).exists(deletePath))
				deletePath.getFileSystem(conf).delete(deletePath, true);
			iterationCount++;
		}
		return 0;
	}
/**
 * This method return ture if current iteration is less the the max iteration 
 * @param iterationCount
 * @param maxIterations
 *
 * */
	private boolean isIterationOver(int iterationCount, long maxIterations) {
		return iterationCount < maxIterations;
	}

	
	/**
	 * 
	 * @param args
	 * Expected arguments are :
	 * args[0] input directory 
	 * args[1] output directory 
	 * args[2]  Max iteration 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SingleSourceShortestPathMR(), args);
		System.exit(res);
	}

}
