package org.janelia.sort.tsp.conversion;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 * 
 * Transfer similarity to distance d using a sigmoid-like function:
 * 
 * d = f / ( s + exp(-|similarity - 1.0|)
 *
 */
public class SimilarityToDistanceSigmoid implements
		SimilarityToDistanceInterface {
	
	private final double factor;
	private final double summand;
	private final double nanReplacement;

	/**
	 * @param factor f
	 * @param summand s
	 * @param nanReplacement replace NaN similarity values with this value instead
	 */
	public SimilarityToDistanceSigmoid(final double factor, final double summand,
			final double nanReplacement) {
		super();
		this.factor = factor;
		this.summand = summand;
		this.nanReplacement = nanReplacement;
	}
	
	

	/**
	 * @param factor f
	 */
	public SimilarityToDistanceSigmoid( final double factor ) {
		this( factor, 0.0, 1000000 );
	}


	@Override
	public double convert(final double similarity) {
		if ( Double.isNaN( similarity ) )
            return this.nanReplacement;
		else {
			final double absDiff = Math.abs( 1.0 - similarity );
			return this.factor * 1.0 / ( this.summand + Math.exp( -absDiff ) );
		}
	}

}
