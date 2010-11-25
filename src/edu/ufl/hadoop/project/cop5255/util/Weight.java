/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.util;

/**
 * @author Owner
 *
 */
public class Weight {

	private Double weight;

	public static final Weight ZERO = new Weight(0d);
	
	public Weight(Double weight) {
		super();
		this.weight = weight;
	}

	public Double getWeightInDouble() {
		return weight;
	}
	
	private void setWeightInDouble(Double weight) {
		this.weight = weight; 
	}

	public void setInfinity() {
		this.setWeightInDouble(Double.POSITIVE_INFINITY);
	}

	public boolean isInfinity() {
		return this.equals(Double.POSITIVE_INFINITY);
	}
}
