/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.util;

/**
 * @author Owner
 *
 */
public class Edge {

	private GraphNode toNode;
	
	private GraphNode fromNode;
	
	private Weight weight;

	public Edge(GraphNode toNode, GraphNode fromNode, Double weight) {
		super();
		this.toNode = toNode;
		this.fromNode = fromNode;
		this.weight = new Weight(weight);
	}
	
	public Edge(GraphNode toNode, GraphNode fromNode, Weight weight) {
		super();
		this.toNode = toNode;
		this.fromNode = fromNode;
		this.weight = weight;
	}

	public GraphNode getToNode() {
		return toNode;
	}

	public GraphNode getFromNode() {
		return fromNode;
	}

	public Weight getWeight() {
		return weight;
	}
	
	public Double getWeightInDouble() {
		return weight.getWeight();
	}
	
}
