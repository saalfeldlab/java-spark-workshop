/**
 * 
 */
package org.janelia.workshop.spark.utility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import scala.Tuple2;
import net.imglib2.KDTree;
import net.imglib2.RealPoint;
import net.imglib2.Sampler;
import net.imglib2.neighborsearch.KNearestNeighborSearchOnKDTree;

/**
 * @author hanslovskyp
 *
 */
public class KNearestNeighbors1D {
	
	private final int k;
	private final KDTree< Integer > tree;
	
	/**
	 * @param k
	 */
	public KNearestNeighbors1D( int k, double[] interval ) {
		super();
		this.k = k;
		this.tree = createTree( interval );
	}
	
	
	public int[][] getNeighbors( final double[] positions ) throws InterruptedException {
		int nThreads = Runtime.getRuntime().availableProcessors();
		return getNeighbors( positions, nThreads );
	}
	
	
	public int[][] getNeighbors( final double[] positions, final int nThreads ) throws InterruptedException {
		int[][] result = new int[ positions.length ][ this.k ];
		ArrayList< Callable< Void > > callables = new ArrayList<Callable<Void>>();
		for (int i = 0; i < result.length; i++) {
			final int[] resultAt = result[i];
			final int k          = this.k;
			final RealPoint ref  = new RealPoint( positions[i] );
			callables.add( new Callable<Void>() {

				public Void call() throws Exception {
					KNearestNeighborSearchOnKDTree< Integer > knn = new KNearestNeighborSearchOnKDTree< Integer >( tree, k );
					knn.search( ref );
					for ( int j = 0; j < k; ++j ) {
						Sampler<Integer> val = knn.getSampler( j );
						resultAt[ j ] = val.get().intValue();
					}
					return null;
				}
			});
		}
		ExecutorService es = Executors.newFixedThreadPool( nThreads );
		es.invokeAll( callables );
		es.shutdown();
		return result;
	}
	
	
	public static KDTree< Integer > createTree( final double[] interval ) {
		ArrayList<Integer> values = new ArrayList< Integer >();
		ArrayList<RealPoint> positions = new ArrayList< RealPoint >();
		for (int i = 0; i < interval.length; i++) {
			values.add( i );
			positions.add( new RealPoint( interval[i] ) );
		}
		return new KDTree<Integer>(values, positions);
	}
	
	
	public static void main(String[] args) throws InterruptedException {
		double[] transform = new double[] { 0, 1 ,2 ,10, 3, 4, 7, 5, 6, 8, 9, 11 };
		KNearestNeighbors1D knn = new KNearestNeighbors1D( 4, transform );
		int[][] result = knn.getNeighbors( transform );
		System.out.println( Arrays.toString( transform ) );
		for (int i = 0; i < result.length; i++) {
			System.out.print( new Tuple2< Integer, Double >( i, transform[i] ) + ":" );
			for (int j = 0; j < result[i].length; j++) {
				System.out.print( " " + new Tuple2< Integer, Double >( result[i][j], transform[result[i][j]] ) );
			}
			System.out.println();
		}
	}
	

}
