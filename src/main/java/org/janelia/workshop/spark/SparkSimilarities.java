package org.janelia.workshop.spark;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.workshop.spark.utility.FloatProcessorInfo;
import org.janelia.workshop.spark.utility.Functions;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 */
public class SparkSimilarities {
	
	
	public static class Options { 
		
		@Option(name = "-f", aliases = { "--format" }, required = true,
				usage = "Format string for input files.")
		private String format;
		
		@Option(name = "-o", aliases = { "--output" }, required = true,
				usage = "Output path for resulting matrix.")
		private String output;
		
		private final Integer defaultMinimum = 0;
		@Option(name = "-m", aliases = { "--minimum-index" }, required = false,
				usage = ( "Minimum index for replacement in format string (inclusive), defaults to " + 0 ) )
		private Integer minimum;
		
		@Option(name = "-M", aliases = { "--maximum-index" }, required = true,
				usage = "Maximum index for replacement in format string (exclusive)." )
		private Integer maximum;
		
		@Option(name = "-r", aliases = { "--range" }, required = false,
				usage = "Correlation range. Must be smaller or equal than and defaults to <maximum-index> - <minimum-index>" )
		private Integer range;
		
		@Option(name = "-s", aliases = { "--scale" }, required = false,
				usage = "Downscale images by a factor of 2^<scale> before calculating matrix. <scale> defaults to 0." )
		private Integer scale;
		
		@Option( name = "-F", aliases = { "--from-file" }, required = false,
				 usage = "If specified, -f will be interpreted as path to file that contains all filenames instead of format string." )
		private Boolean fromFile = false;
				 
		
		
		private boolean parsedSuccessfully = false;
		
		public Options( String[] args ) {
			CmdLineParser parser = new CmdLineParser( this );
			try {
				parser.parseArgument( args );
				this.minimum       = this.minimum == null ? this.defaultMinimum : this.minimum;
				final int maxRange = this.maximum - this.minimum - 1;
				this.range         = this.range   == null ? maxRange : this.range;
				
				if ( this.range > maxRange ) throw new CmdLineException( parser, "--range cannot be larger than " + maxRange, new Exception() );
				
				parsedSuccessfully = true;
			} catch (CmdLineException e ) {
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}

		public String getFormat() {
			return format;
		}

		public String getOutput() {
			return output;
		}

		public Integer getMinimum() {
			return minimum;
		}

		public Integer getMaximum() {
			return maximum;
		}

		public Integer getRange() {
			return range;
		}

		public Integer getScale() {
			return scale;
		}

		public Boolean getFromFile() {
			return fromFile;
		}

		public boolean isParsedSuccessfully() {
			return parsedSuccessfully;
		}
		
	}
	
	
	public static void main(String[] args) throws IOException {
		
		Options o = new Options(args);
		if ( o.isParsedSuccessfully() ) {
		
			final SparkConf conf      = new SparkConf().setAppName("Similarity Matrix calculation");
	        final JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        final int start        = o.getMinimum();
	        final int stop         = o.getMaximum();
	        final int range        = o.getRange();
	        final String format    = o.getFormat();
	        final int sampleScale  = o.getScale();
	        final String output    = o.getOutput();
	        final Boolean fromFile = o.getFromFile();
	        final int size;
	        
	        
	        ArrayList<String> filenames = new ArrayList< String >();
	        if ( fromFile ) {
	        	Scanner s = new Scanner(new File( format ));
	        	ArrayList<String> list = new ArrayList<String>();
	        	while (s.hasNext()) list.add(s.next());
	        	s.close();
	        	int lower = Math.min( start, list.size() );
	        	int upper = Math.min( stop, list.size() );
	        	size = upper - lower;
	        	for ( int l = lower; l < upper; ++l ) filenames.add( list.get( l ) );
	        } else {
	        	size = stop - start;
	        	for ( int s = start; s < stop; ++s )
	        		filenames.add( String.format( format, s ) );
	        }
	        
	        ArrayList< Tuple2< Integer, String > > filenamesWithIndices = new ArrayList< Tuple2< Integer, String > >();
	        for ( int i = 0;  i < size; ++i )
	        	filenamesWithIndices.add( new Tuple2<Integer, String>( i, filenames.get( i ) ) );
	        
	        for ( Tuple2<Integer, String> fwi : filenamesWithIndices )
	        	System.out.println( fwi._1() + " " + fwi._2() );
	        
	        
	        System.out.println( String.format( "Processing %d filenames with range=%d and defaultParallelism=%d.", filenames.size(), range, sc.defaultParallelism() ) );
	        
	        for ( Tuple2<Integer, String> fnwi : filenamesWithIndices )
	        	System.out.println( fnwi );
     

			final JavaPairRDD<Integer, String> filenamesRDD            = sc.parallelizePairs( filenamesWithIndices );
			final JavaPairRDD<Integer, FloatProcessorInfo> filesRDD    = filenamesRDD.mapToPair( new Functions.LoadFile( ) )
					.mapToPair( new Functions.ReplaceValue( 0.0f, Float.NaN ) )
					.mapToPair( new Functions.DownSample( sampleScale ) )
					.cache();
			JavaPairRDD<Tuple2<Integer, Integer>, Double> similarities = filesRDD
					.cartesian( filesRDD )
					.filter( new Functions.RangeFilter( range ) )
					.mapToPair( new Functions.PairwiseSimilarity() );
			
			
        
			long t0                                               = System.currentTimeMillis();
	        List<Tuple2<Tuple2<Integer, Integer>, Double>> values = similarities.collect();
	        long sparkTime                                        = System.currentTimeMillis();
	        
	        
	        
	        FloatProcessor matrix = new FloatProcessor( size, size );
	        matrix.add( Double.NaN );
	        for ( int i = 0; i < size; ++i )
	        	matrix.setf( i, i, 1.0f );
	        
	        for ( Tuple2<Tuple2<Integer, Integer>, Double> v : values ) {
	        	Tuple2<Integer, Integer> pair = v._1();
	        	int i1 = pair._1();
	        	int i2 = pair._2(); // - i1;
	        	float val = v._2().floatValue();
	        	matrix.setf( i1, i2, val );
	        	matrix.setf( i2, i1, val );
	        }
	        
	        long matrixTime = System.currentTimeMillis();
	        
	        new FileSaver( new ImagePlus( "", matrix ) ).saveAsTiff( output );
	        
	        System.out.println( String.format( "%-50s% 20dms", "Spark computation time:", ( sparkTime - t0 ) ) );
	        System.out.println( String.format( "%-50s% 20dms", "Matrix filling time:", ( matrixTime - sparkTime ) ) );
	        
	        sc.close();
		}
	}

}
