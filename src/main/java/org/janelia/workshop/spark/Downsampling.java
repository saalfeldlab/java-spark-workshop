package org.janelia.workshop.spark;

import ij.ImagePlus;
import ij.process.FloatProcessor;

import java.util.ArrayList;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.workshop.spark.utility.FloatProcessorInfo;
import org.janelia.workshop.spark.utility.Functions;

import scala.Tuple2;

public class Downsampling {
	
	public static int sparkMainFunction( JavaSparkContext sc, String[] args ) {
		final String inputFormat  = args[0];
		final int min             = Integer.parseInt( args[1] );
		final int max             = Integer.parseInt( args[2] );
		final int level           = Integer.parseInt( args[3] );
		final String outputFormat = args[4];
		
		ArrayList<Integer> indices = new ArrayList< Integer >();
		for ( int m = min; m < max; ++m )
			indices.add( m );
		
		
		Integer nWrittenFiles = sc.parallelize( indices )
				.mapToPair( new PairFunction<Integer, Integer, FloatProcessorInfo>() {

					private static final long serialVersionUID = 1L;
					public Tuple2<Integer, FloatProcessorInfo> call(Integer t)
					throws Exception {
						FloatProcessor fp = new ImagePlus( String.format( inputFormat, t.intValue() ) ).getProcessor().convertToFloatProcessor();
						return new Tuple2< Integer, FloatProcessorInfo >( t, new FloatProcessorInfo( fp ) );
					}
				})
				.mapToPair( new Functions.DownSample( level ) )
				.aggregate( new Integer( 0 ),
						new Functions.WriteToFormatStringAndCountOnSuccess( outputFormat ),
						new Functions.MergeCount() )
        		;
		return nWrittenFiles;
	}
	
	public static void main(String[] args) {
		SparkConf conf    = new SparkConf().setAppName( "Downsampling!" );
		long tStart       = System.currentTimeMillis();
		int nWrittenFiles = sparkMainFunction( new JavaSparkContext( conf ), args );
		long tStop        = System.currentTimeMillis();
		long tDiff        = tStop - tStart;
		String hms        = DurationFormatUtils.formatDuration( tDiff, "HH:mm:ss.SSS" );
		System.out.println( "Successfully wrote " + nWrittenFiles + " images in " + hms );
	}

}
