package edu.ufl.hadoop.project.cop5255.sssp;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import edu.ufl.hadoop.project.cop5255.util.Edge;

import edu.ufl.hadoop.project.cop5255.util.NodeColor;
import edu.ufl.hadoop.project.cop5255.util.Weight;

public class Node {

	private final long id;
	private Double distance;
	private List<Edge> edges = new ArrayList<Edge>();
	private NodeColor color = NodeColor.WHITE;

	public Node(String str) {
		Pattern pattern = Pattern.compile("(.*?)\t(.*)");
		Matcher matcher = pattern.matcher(str);
		matcher.find();
		id = Long.parseLong(matcher.group(1).trim());
		StringBuilder builder = new StringBuilder();
		int totalCount = matcher.group(2).trim().split(",").length;
		for (int i = 0; i < totalCount; i++) {
			builder.append("(.*?)");
			if (i != totalCount - 1) {
				builder.append(",");
			}
		}
		builder.append("\\|(.*?)\\|(.*)");
		pattern = Pattern.compile(builder.toString());
		matcher = pattern.matcher(matcher.group(2).trim());
		matcher.find();
		for (int i = 0; i < totalCount; i++) {
			Pattern weightPattern = Pattern.compile("\\(+(.*)\\.+");
			Matcher weightMatcher = weightPattern.matcher(matcher.group(i + 1));
			weightMatcher.find();
			System.out.println("Weight"
					+ matcher.group(i + 1));
			Double distance = matcher.group(i + 1).trim().equals("") ? 999d : Double.valueOf(weightMatcher.group().substring(1,
					weightMatcher.group().length() - 1));
			System.out.println("To node : " + matcher.group(i + 1));
			String toNode = matcher.group(i + 1);
			if(!toNode.trim().equals("")) {
			this.addEdge(new Edge( 
					new Node(Long.valueOf(toNode.substring(0, toNode.indexOf("("))), NodeColor.BLACK, distance), 
					new Node(id, NodeColor.BLACK, Double.MIN_VALUE), distance));
			}
			
		}
		System.out.println("Weight : " + matcher.group(totalCount + 1));
		System.out.println("Color :" + matcher.group(totalCount + 2));
		this.color = NodeColor.valueOf(matcher.group(totalCount + 2));
		String weightInString = matcher.group(totalCount + 1);
		this.distance = weightInString.equalsIgnoreCase("Integer.MAX_VALUE") ? Double.POSITIVE_INFINITY : Double.valueOf(weightInString); 
	}

	public Node(long id) {
		this.id = id;
	}

	public Node(long Id, NodeColor nodeColor, Double distance) {
		this.color = nodeColor;
		this.id = Id;
		this.distance = distance;
	}

	public long getId() {
		return this.id;
	}

	public Double getDistance() {
		return this.distance;
	}

	public void setDistance(Double distance) {
		this.distance = distance;
	}

	public NodeColor getColor() {
		return this.color;
	}

	public void setColor(NodeColor color) {
		this.color = color;
	}

	public List<Edge> getEdges() {
		return this.edges;
	}

	public void addEdge(Edge edge) {
		this.edges.add(edge);
	}

	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}

	public Text getLine() {
		StringBuffer s = new StringBuffer();

		for (Edge v : edges) {
			s.append(v.toString()).append(",");
		}
		s.append("|");

		if (this.distance < Double.MAX_VALUE) {
			s.append(this.distance).append("|");
		} else {
			s.append("Integer.MAX_VALUE").append("|");
		}

		s.append(color.toString());

		return new Text(s.toString());
	}

}
