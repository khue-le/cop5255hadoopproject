/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.util;

import java.util.ArrayList;
import java.util.List;


/**
 * @author Owner
 *
 */
public final class GraphNode {

	private NodeColor nodeColor;
	
	private List<Edge> edges;
	
	private Weight weight;
	
	private Integer Id;

	private GraphNode() {
		edges = new ArrayList<Edge>();
	}
	
	public GraphNode(Integer Id, NodeColor nodeColor, Weight weight) {
		this();
		this.nodeColor = nodeColor;
		this.Id = Id;
		this.weight = weight;
	}
	
	public GraphNode(Integer Id, NodeColor nodeColor, Double weight) {
		this();
		this.nodeColor = nodeColor;
		this.Id = Id;
		this.weight = new Weight(weight);
	}

	public NodeColor getNodeColor() {
		return nodeColor;
	}

	public List<Edge> getEdges() {
		return edges;
	}

	public boolean addEdge(GraphNode fromNode, Double weight) {
		return this.addEdge(new Edge(this, fromNode, weight));
	}
	
	public boolean addEdges(List<Edge> edges) {
		return this.edges.addAll(edges);
	}
	
	private boolean addEdge(Edge edge) {
		return this.edges.add(edge);
	}

	public Weight getWeight() {
		return weight;
	}

	public Integer getId() {
		return Id;
	}
	
	public void updateColor(NodeColor color) {
		this.nodeColor = color; 
	}
}
