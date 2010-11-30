/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.util;

import edu.ufl.hadoop.project.cop5255.sssp.Node;

/**
 * @author Owner
 *
 */
public class Edge {

	private Node toNode;
	
	private Node fromNode;
	
	private Weight weight;

	public Edge(Node toNode, Node fromNode, Double weight) {
		super();
		this.toNode = toNode;
		this.fromNode = fromNode;
		this.weight = new Weight(weight);
	}
	
	public Edge(Node toNode, Node fromNode, Weight weight) {
		super();
		this.toNode = toNode;
		this.fromNode = fromNode;
		this.weight = weight;
	}

	public Node getToNode() {
		return toNode;
	}

	public Node getFromNode() {
		return fromNode;
	}

	public Weight getWeight() {
		return weight;
	}
	
	public Double getWeightInDouble() {
		return weight.getWeightInDouble();
	}
	
	public String toString() {
		return toNode.getId() + "(" + getWeightInDouble() + ")";
	}
}
