package edu.ufl.hadoop.project.cop5255.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class InputPathFilter implements PathFilter {

	private static final String DEFAULT_FILE_NAME = "docs";
	
	@Override
	public boolean accept(Path paramPath) {
		System.out.println("Param Path hai na yaar for " + paramPath.getName() + " has status: " + DEFAULT_FILE_NAME.equalsIgnoreCase(paramPath.getName().trim()));
		return DEFAULT_FILE_NAME.equalsIgnoreCase(paramPath.getName().trim());
	}
	
}