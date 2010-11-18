/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.builder;

import java.util.Random;

import edu.ufl.hadoop.project.cop5255.util.Edge;
import edu.ufl.hadoop.project.cop5255.util.GraphNode;
import edu.ufl.hadoop.project.cop5255.util.NodeColor;

/**
 * @author Owner
 *
 */
public class EdgeBuilder {

	private static final int RANDOM_UPPER_LIMIT_WEIGHT = 100;
	
	public static Edge build(long noOfNodes) {
		Random random = new Random();
		long node1 = 0;
		long node2 = 0;
		do {
			node1 = random.nextLong() % noOfNodes;
			node2 = random.nextLong() % noOfNodes;
		} while(node1 <= 0 || node2 <= 0 || node1 == node2);
 		return new Edge(
 				new GraphNode(node1, NodeColor.WHITE, Double.POSITIVE_INFINITY), 
 				new GraphNode(node2, NodeColor.WHITE, Double.POSITIVE_INFINITY), 
 				Double.valueOf(random.nextInt(RANDOM_UPPER_LIMIT_WEIGHT)
 				)
 			);
	}
}
