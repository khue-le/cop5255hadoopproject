/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.util;

/**
 * The Enum NodeColor.
 * 
 * @author Owner
 */
public enum NodeColor {

	/** The GRAY. */
	GRAY(1),
	/** The BLACK. */
	BLACK(2),
	/** The WHITE. */
	WHITE(0);

	/** The color index. */
	private int colorIndex;

	/**
	 * Instantiates a new node color.
	 * 
	 * @param colorIndex
	 *            the color index
	 */
	private NodeColor(int colorIndex) {
		this.colorIndex = colorIndex;
	}

	/**
	 * Checks if is same color.
	 * 
	 * @param color
	 *            the color
	 */
	public boolean equals(NodeColor color) {
		return this.getColorIndex() == color.getColorIndex();
	}

	public static boolean equals(NodeColor color1, NodeColor color2) {
		return color1 != null && color2 != null
				&& color1.getColorIndex() == color2.getColorIndex();
	}

	/**
	 * Gets the color index.
	 * 
	 * @return the color index
	 */
	public int getColorIndex() {
		return colorIndex;
	}

}
