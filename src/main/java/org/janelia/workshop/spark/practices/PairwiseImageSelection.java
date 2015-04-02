/**
 * 
 */
package org.janelia.workshop.spark.practices;

import java.util.ArrayList;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.workshop.spark.utility.FloatProcessorInfo;

import scala.Tuple2;
import scala.Tuple3;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 */
public class PairwiseImageSelection {
	
	@SuppressWarnings("serial")
	public static void selectImages( final JavaSparkContext sc, final int nImages, final int sideLength, final int range ) {
		
		ArrayList<Integer> indices = new ArrayList< Integer >();
		for ( int i = 0; i < nImages; ++i ) indices.add( i );
		// create images with index
		JavaPairRDD<Integer, FloatProcessorInfo> indexedFiles = sc
				.parallelize( indices )
				.mapToPair( new PairFunction<Integer, Integer, FloatProcessorInfo >() {
					public Tuple2<Integer, FloatProcessorInfo> call(Integer t)
							throws Exception {
						return new Tuple2< Integer, FloatProcessorInfo >( t, new FloatProcessorInfo( sideLength, sideLength, new float[ sideLength*sideLength ] ) );
					}
				})
				;
		
		// bad practice (cartesian)
		long tStartBadPractice = System.currentTimeMillis();
		JavaPairRDD<Tuple2<Integer, FloatProcessorInfo>, Tuple2<Integer, FloatProcessorInfo>> pairsBadPractice = indexedFiles
				.cartesian( indexedFiles )
				.filter( new Function<Tuple2<Tuple2<Integer,FloatProcessorInfo>,Tuple2<Integer,FloatProcessorInfo>>, Boolean>() {
					
					public Boolean call(
							Tuple2<Tuple2<Integer, FloatProcessorInfo>, Tuple2<Integer, FloatProcessorInfo>> v1)
							throws Exception {
						return ( ( v1._1()._1().intValue() - v1._2()._1().intValue() ) < range ) && ( v1._1()._1().intValue() > v1._2()._1().intValue() ); 
					}
				}).cache();
				;
		long countBadPractice = pairsBadPractice.count();
		long tEndBadPractice  = System.currentTimeMillis();
		long tDiffBadPractice = tEndBadPractice - tStartBadPractice;
		String hmsBadPractice = DurationFormatUtils.formatDuration( tDiffBadPractice, "HH:mm:ss.SSS" );
		
		
		// good practice (using look-up table, broadcast and flatMap)
		long tStartGoodPractice = System.currentTimeMillis();
		ArrayList< ArrayList< Integer > > pairs = new ArrayList< ArrayList< Integer > >();
		for ( int i = 0; i < nImages; ++i ) {
			ArrayList<Integer> matches = new ArrayList< Integer >();
			pairs.add( matches );
			for ( int k = i + 1; k < nImages && k - i < range; ++k )
				matches.add( k );
		}
		final Broadcast<ArrayList<ArrayList<Integer>>> pairsBC = sc.broadcast( pairs );
		
		JavaPairRDD<Integer, Tuple2<Tuple2<Integer, FloatProcessorInfo>, FloatProcessorInfo>> pairsGoodPractice = indexedFiles
				.map( new Function<Tuple2<Integer,FloatProcessorInfo>, Tuple3< Integer, FloatProcessorInfo, ArrayList< Integer > > >() {
					// map ( i, image ) -> ( i, image, [...,k,...] )
					public Tuple3<Integer, FloatProcessorInfo, ArrayList< Integer > > call(
							Tuple2<Integer, FloatProcessorInfo> v1)
							throws Exception {
						return new Tuple3<Integer, FloatProcessorInfo, ArrayList<Integer>>( v1._1(), v1._2(), pairsBC.getValue().get( v1._1() ) );
					}
				})
				.flatMapToPair( new PairFlatMapFunction<Tuple3<Integer,FloatProcessorInfo,ArrayList<Integer>>, Integer, Tuple2< Integer, FloatProcessorInfo > >() {
					// map flat ( i, image, [...,k,...] ) -> ( k : ( i, image ) )
					public Iterable<Tuple2<Integer, Tuple2<Integer, FloatProcessorInfo>>> call(
							Tuple3<Integer, FloatProcessorInfo, ArrayList<Integer>> t)
							throws Exception {
						ArrayList<Tuple2<Integer, Tuple2<Integer, FloatProcessorInfo>>> res = new ArrayList< Tuple2< Integer, Tuple2< Integer, FloatProcessorInfo > > >();
						for ( Integer i : t._3() )
							res.add( new Tuple2<Integer, Tuple2<Integer,FloatProcessorInfo>>( 
									i,
									new Tuple2< Integer, FloatProcessorInfo >( t._1(), t._2() )
									) );
						return res;
					}
				})
				.join( indexedFiles ) // map ( k : ( i, image ) ) -> ( k : ( ( i, image ), image ) )
				;
		
		
		long countGoodPractice = pairsGoodPractice.count();
		long tEndGoodPractice  = System.currentTimeMillis();
		long tDiffGoodPractice = tEndGoodPractice - tStartGoodPractice;
		String hmsGoodPractice = DurationFormatUtils.formatDuration( tDiffGoodPractice, "HH:mm:ss.SSS" );
		
		String format = "Selected %d image pairs in %s (%s practice).";
		System.out.println( String.format( format, countBadPractice,  hmsBadPractice,  "bad"  ) );
		System.out.println( String.format( format, countGoodPractice, hmsGoodPractice, "good" ) );
		
		
	}
	
	public static void main(String[] args) {
		int nImages = Integer.parseInt( args[0] );
		int sideLength = Integer.parseInt( args[1] );
		int range = Integer.parseInt( args[2] );
		SparkConf conf = new SparkConf().setAppName( "Best practice for pairwise image selection" );
		JavaSparkContext sc = new JavaSparkContext( conf );
		System.out.println( "Run bad practice (cartesian) and good practice (flatMap) examples for sparse pairwise image selection." );
		selectImages( sc, nImages, sideLength, range );
	}

}
