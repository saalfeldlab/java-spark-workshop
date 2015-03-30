/**
 * 
 */
package org.janelia.workshop.spark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.workshop.spark.utility.FloatProcessorInfo;
import org.janelia.workshop.spark.utility.Functions;
import org.janelia.workshop.spark.utility.KNearestNeighbors1D;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;

/**
 * @author hanslovskyp
 *
 */
public class InjectionRendering {
	
public static class Options { 
		
		@Option(name = "-f", aliases = { "--format" }, required = true,
				usage = "Format string for input files.")
		private String format;
		
		@Option(name = "-o", aliases = { "--output" }, required = true,
				usage = "Output format for resulting images.")
		private String output;
		
		private final Integer defaultMinimum = 0;
		@Option(name = "-m", aliases = { "--minimum-index" }, required = false,
				usage = ( "Minimum index for replacement in format string (inclusive), defaults to " + 0 ) )
		private Integer minimum;
		
		@Option(name = "-M", aliases = { "--maximum-index" }, required = true,
				usage = "Maximum index for replacement in format string (exclusive)." )
		private Integer maximum;
		
		@Option( name = "-F", aliases = { "--from-file" }, required = false,
				 usage = "If specified, -f will be interpreted as path to file that contains all filenames instead of format string." )
		private Boolean fromFile = false;
		
		@Option( name = "-c", aliases = { "--coordinate-transform" }, required = true,
				 usage = "Path to file containing coordinate transform as csv file." )
		private String coordinateTransform;
		
		@Option( name = "-n", aliases = { "--n-scans-per-section" }, required = false,
				 usage = "On average, how many images should be merged into one target image? (default=1)" )
		private Integer nScansPerSection = 1;
		
		@Option( name = "-k", aliases = { "--k-nearest-neighbors" }, required = true,
				 usage = "Nubmer of nearest neighbors used for rendering." )
		private Integer kNearestNeighbors;
				 
		
		private boolean parsedSuccessfully = false;
		
		public Options( String[] args ) {
			CmdLineParser parser = new CmdLineParser( this );
			try {
				parser.parseArgument( args );
				this.minimum       = this.minimum == null ? this.defaultMinimum : this.minimum;
				if ( this.nScansPerSection < 1 )  throw new CmdLineException( parser, "--n-scans-per-section/-n must be greater than or equal to 1 (is " + nScansPerSection + ")", new Exception() );
				if ( this.kNearestNeighbors < 1 ) throw new CmdLineException( parser, "--k-nearest-neighbors/-k must be greater than or equal to 1 (is " + nScansPerSection + ")", new Exception() );
				
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

		public Boolean getFromFile() {
			return fromFile;
		}
		
		public String getCoordinateTransform() {
			return coordinateTransform;
		}

		public boolean isParsedSuccessfully() {
			return parsedSuccessfully;
		}

		public Integer getnScansPerSection() {
			return nScansPerSection;
		}
		
		public Integer getkNearestNeighbors() {
			return kNearestNeighbors;
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		Options o = new Options(args);
		if ( o.isParsedSuccessfully() ) {
			
			int nScansPerSection = o.getnScansPerSection();
			Integer kNearestNeighbors = o.getkNearestNeighbors();
			double scale = 1.0 / nScansPerSection;
			String transformFileName = o.getCoordinateTransform();
			String inputFormat = o.getFormat();
			String outputFormat = o.getOutput();
			Boolean fromFile = o.getFromFile();
			int start = o.getMinimum();
			int stop  = o.getMaximum();
			
	        ArrayList<Tuple2<Integer, String>> indexedFilenames = new ArrayList< Tuple2< Integer, String > >();
	        if ( fromFile ) {
	        	Scanner s = new Scanner(new File( inputFormat ));
	        	ArrayList<String> list = new ArrayList<String>();
	        	while (s.hasNext()) list.add(s.next());
	        	s.close();
	        	int lower = Math.min( start, list.size() );
	        	int upper = Math.min( stop, list.size() );
	        	for ( int l = lower; l < upper; ++l ) indexedFilenames.add( new Tuple2< Integer, String > ( l, list.get( l ) ) );
	        } else {
	        	for ( int s = start; s < stop; ++s )
	        		indexedFilenames.add( new Tuple2< Integer, String >( s, String.format( inputFormat, s ) ) );
	        }
	        
	        final SparkConf conf      = new SparkConf().setAppName( "Coordinate transform image rendering calculation" );
	        final JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        long tStart = System.currentTimeMillis();
	        
	        
	        JavaPairRDD<Integer, FloatProcessorInfo> files = sc.parallelizePairs( indexedFilenames )
	        		.mapToPair( new Functions.LoadFile() )
	        		.cache()
	        		;
	        
	        
	        Scanner scanner = new Scanner(new File( transformFileName ) );
        	ArrayList< Tuple2< Integer, String> > transformStringsFull = new ArrayList< Tuple2< Integer, String> >();
        	for ( int i = 0; scanner.hasNext(); ++i ) transformStringsFull.add( new Tuple2< Integer, String >( i, scanner.next() ));
        	scanner.close();
        	ArrayList< Tuple2< Integer, String > > transformStrings = new ArrayList<Tuple2<Integer,String>>();
        	for ( int i = start; i < stop; ++i ) transformStrings.add( transformStringsFull.get( i ) );
        	double minVal = Double.parseDouble( transformStrings.get( 0 )._2() );
        	
        	Functions.DoubleComparator comp = new Functions.DoubleComparator();
        	
	        JavaPairRDD< Integer, String > coordinateTransformStrings = sc.parallelizePairs( transformStrings, Math.min( 60*16, sc.defaultParallelism() ) ); // TODO need to find a better way to do this, maybe local?
	        JavaPairRDD< Integer, Double > coordinateTransform = coordinateTransformStrings
	        		.mapToPair( new Functions.ExtractFromCsv() )
	        		.mapToPair( new Functions.Add( -minVal ) )
	        		.mapToPair( new Functions.Scale( scale ) )
	        		.cache()
	        		;
	        
	        
	        Double min                 = coordinateTransform.map( new Functions.DoubleOnly< Integer >() ).min( comp );
	        Double max                 = coordinateTransform.map( new Functions.DoubleOnly< Integer >() ).max( comp );
	        List<Double> transformList = coordinateTransform.map( new Functions.DoubleOnly< Integer >() ).collect();
	        
	        int nTargetPixels = (int) ( ( max - min ) ) + 1;
	        ArrayList<Integer> targetIndicesList = new ArrayList< Integer >();
	        for ( int i = 0; i < nTargetPixels; ++i ) targetIndicesList.add( i );
	        double transform[] = new double[ transformList.size() ];
	        for ( int i = 0 ; i < transformList.size(); ++i ) transform[i] = transformList.get( i );
	        
	        double[] targetPositions = new double[ nTargetPixels ];
	        for (int i = 0; i < targetPositions.length; i++ ) targetPositions[ i ] = i;
	        final int[][] nearestNeighbors = new KNearestNeighbors1D( kNearestNeighbors, transform ).getNeighbors( targetPositions );
	        Functions.TransformIndexPairsToIndexPairWithWeight calcVal = new Functions.TransformIndexPairsToIndexPairWithWeight( 0.5 );
	        List<Tuple2<Integer, ArrayList<Tuple2<Integer, Double>>>> sourceMapsToMultipleTargets = new ArrayList< Tuple2< Integer, ArrayList< Tuple2< Integer, Double > > > >();
	        for ( int i = 0; i < transform.length; ++i ) 
	        	sourceMapsToMultipleTargets.add( new Tuple2< Integer, ArrayList< Tuple2< Integer, Double > > >( i, new ArrayList< Tuple2< Integer, Double > >() ) );
	        for ( int i = 0; i < nearestNeighbors.length; ++i ) {
	        	int[] nearestNeighborsFor = nearestNeighbors[i];
	        	for ( int k = 0; k < nearestNeighborsFor.length; ++k ) {
	        		int sourceIndex = nearestNeighborsFor[k];
	        		sourceMapsToMultipleTargets.get( sourceIndex )._2.add( new Tuple2<Integer, Double>( i, calcVal.gaussOffset( transform[sourceIndex], i) ) );
	        	}
	        }
	        	
	        
	        Broadcast<List<Tuple2<Integer, ArrayList<Tuple2<Integer, Double>>>>> bc = sc.broadcast( sourceMapsToMultipleTargets );
	        
	        
	        JavaRDD<Tuple2<FloatProcessorInfo, ArrayList<Tuple2<Integer, Double>>>> imagesWithWeightedSourceToTargetMappings = files
	        		.map( new Functions.CombineByKeyWithBroadcast< FloatProcessorInfo, ArrayList< Tuple2< Integer, Double > > >( bc ) )
	        		;

	        
	        JavaPairRDD<Integer, Tuple2<FloatProcessorInfo, Double>> targetToImageMappingWithWeight = imagesWithWeightedSourceToTargetMappings
	        		.flatMap( new Functions.FlattenFloatProcessorInfoWithTargetList< Integer, Double >() )
	        		.mapToPair( new Functions.TripleToKeyPair< Integer, FloatProcessorInfo, Double >() );

	        
	        JavaPairRDD<Integer, FloatProcessorInfo> resultIgnoredBlack = targetToImageMappingWithWeight
	        		.combineByKey(
	        				new Functions.CreateFloatProcessorInfoWithWeightSum(), 
	        				new Functions.AddWeightedFloatProcessorInfoWithWeightSum(), 
	        				new Functions.MergeWeightedFloatProcessorInfoWithWeightSum()
	        				)
	        		.mapToPair( new Functions.NormalizeFloatProcessorInfo() )
	        		; 
	        
	        
	        Integer goodFiles = resultIgnoredBlack.aggregate( new Integer( 0 ),
	        		new Functions.WriteToFormatStringAndCountOnSuccess( outputFormat ),
	        		new Functions.MergeCount() )
	        		;
	        
	        long tEnd = System.currentTimeMillis();
	        long tDiff = tEnd - tStart;
	        
	        String hms = DurationFormatUtils.formatDuration( tDiff, "HH:mm:ss.SSS" );
	        
	        System.out.println( String.format( "Wrote %d/%d files in %s (%dms).", goodFiles, nTargetPixels, hms, tDiff ) );
			
			sc.close();
		}

	}

}
