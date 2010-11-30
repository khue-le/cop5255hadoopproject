/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.driver;

import org.apache.hadoop.util.ProgramDriver;

import edu.ufl.hadoop.project.cop5255.builder.GraphBuilder;
import edu.ufl.hadoop.project.cop5255.sssp.SingleSourceShortestPathMR;

/**
 * @author Owner
 *
 */
public class MainExecutingDriver {

	  public static void main(String... arguments){
		    int exitCode = -1;
		    ProgramDriver programDriver = new ProgramDriver();
		    try {
		    	programDriver.addClass("generate-map", GraphBuilder.class, "Generates the graph having atmost pre-defined number of edges.");
		    	programDriver.addClass("sssp", SingleSourceShortestPathMR.class, "Single Source Shortest Path algorithm implemented on huge graph");
		    	programDriver.driver(arguments);
		    } catch(Throwable e) {
		    	e.printStackTrace();
		    }
		    System.exit(exitCode);
	  }
}
