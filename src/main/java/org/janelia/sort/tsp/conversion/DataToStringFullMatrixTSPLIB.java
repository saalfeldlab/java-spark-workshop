/**
 * 
 */
package org.janelia.sort.tsp.conversion;


/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 * 
 * Transfer data matrix into tsplib format (<a href="http://comopt.ifi.uni-heidelberg.de/software/TSPLIB95/"</a>),
 * specifying the full matrix, for use with the concorde traveling salesman (TSP) solver
 * (<a href="http://www.math.uwaterloo.ca/tsp/concorde.html"</a>).
 *
 */
public class DataToStringFullMatrixTSPLIB implements DataToStringInterface {
	
	private int n;
	private int dummyIndex;
	
	private final String baseString;
	
	/**
	 * Create {@link DataToStringFullMatrixTSPLIB} object with header.
	 * @param baseString File Header with useful meta information
	 */
	public DataToStringFullMatrixTSPLIB(final String baseString) {
		super();
		this.baseString = baseString;
	}
	

	/**
	 * Create {@link DataToStringFullMatrixTSPLIB} object with default header with empty comment.
	 */
	public DataToStringFullMatrixTSPLIB() {
		this(DataToStringFullMatrixTSPLIB.createBaseStringWithComment( "" )	);
	}

	
	/** Create default header
	 * @param comment {@link String} that gets inserted into comment meta information
	 * @return {@link String} header for {@link DataToStringFullMatrixTSPLIB}
	 */
	public static String createBaseStringWithComment( final String comment ) {
		return "NAME: SORT" + System.getProperty("line.separator")
		+ "TYPE: TSP" + System.getProperty("line.separator")
		+ "COMMENT: " + comment + System.getProperty("line.separator")
		+ "DIMENSION: %d" + System.getProperty("line.separator")
		+ "EDGE_WEIGHT_TYPE: EXPLICIT" + System.getProperty("line.separator")
		+ "EDGE_DATA_FORMAT: EDGE_LIST" + System.getProperty("line.separator")
		+ "EDGE_WEIGHT_FORMAT: FULL_MATRIX" + System.getProperty("line.separator")
		+ "NODE_COORD_TYPE: NO_COORDS" + System.getProperty("line.separator")
		+ "DISPLAY_DATA_TYPE: NO_DISPLAY" + System.getProperty("line.separator")
		+ "EDGE_WEIGHT_SECTION" + System.getProperty("line.separator");
	}

	private double[][] similarities;
	
	private final static double DUMMY_VALUE = 0.0;

	@Override
	public void initialize(final int n) {
		this.n = n + 1;
		this.dummyIndex = n;
		similarities = new double[ this.n ][ this.n ];
	}

	@Override
	public void addSimilarity(final int index1, final int index2, final double value) {
		similarities[index1][index2] = value;
	}

	@Override
	public void addDummy(final int index, final double value) {
		similarities[ index ][ this.dummyIndex ] = DataToStringFullMatrixTSPLIB.DUMMY_VALUE;
	}

	@Override
	public String close() {
		final String base      = String.format( this.baseString, this.n );
		final StringBuilder sb = new StringBuilder( base );
		for ( int i = 0; i < dummyIndex; ++i ) {
			final double[] row = similarities[i];
			for ( int j = 0; j < dummyIndex; ++j ) { 
				sb.append( (int)row[j] ).append( " " );
			}
			sb.append( (int)row[dummyIndex] ).append( "\n" );
		}
		
		// need to append row filled with DUMMY_VALUE for dummy element
		for ( int j = 0; j < dummyIndex; ++j ) {
			sb.append( (int)DataToStringFullMatrixTSPLIB.DUMMY_VALUE ).append(" ");
		}
		sb.append( (int)DataToStringFullMatrixTSPLIB.DUMMY_VALUE ).append("\n").append("EOF");
		return sb.toString();
	}

}
