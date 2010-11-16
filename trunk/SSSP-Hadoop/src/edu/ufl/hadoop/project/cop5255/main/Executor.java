/**
 * 
 */
package edu.ufl.hadoop.project.cop5255.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import edu.ufl.hadoop.project.cop5255.sssp.SingleSourceShortestPathMR;

/**
 * @author Owner
 *
 */
public class Executor {

	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new SingleSourceShortestPathMR(), args));
	}
}
