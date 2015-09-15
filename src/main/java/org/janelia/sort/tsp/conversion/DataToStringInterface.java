/**
 * 
 */
package org.janelia.sort.tsp.conversion;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 * Give rules on how to convert numeric data into {@link String} that can be understood by TSP solver.
 */
public interface DataToStringInterface {
	
	/** 
	 * initialize with number of nodes
	 * @param n number of nodes, excluding dummy node
	 */
	public void initialize( int n );
	
	/**
	 * add edge weight (distance) between two nodes given by index1, index2 with weight value 
	 * @param index1 node1
	 * @param index2 node2
	 * @param value  edge weight (distance)
	 */
	public void addSimilarity( int index1, int index2, double value );
	
	/**
	 * add edge weight (distance) between node given by index and dummy node with weight value
	 * @param index node
	 * @param value edge weight (distance)
	 */
	public void addDummy( int index, double value );
	
	/** close out and return String 
	 * @return {@link String} that contains all information necessary for solving TSP
	 */
	public String close();
	
}
