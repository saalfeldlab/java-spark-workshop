/**
 * 
 */
package org.janelia.sort.tsp.conversion;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 * Define how to derive distance from pairwise similarity measure.
 */
public interface SimilarityToDistanceInterface {
	
	/**
	 * @param similarity Pairwise similarity measure.
	 * @return {@link double} distance that is calculated from similarity 
	 */
	double convert( double similarity );
	
}