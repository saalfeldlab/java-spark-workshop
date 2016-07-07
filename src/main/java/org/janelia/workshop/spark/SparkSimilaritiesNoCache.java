package org.janelia.workshop.spark;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import mpicbg.trakem2.util.Downsampler;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.workshop.spark.utility.FloatProcessorInfo;
import org.janelia.workshop.spark.utility.NCC;
import org.janelia.workshop.spark.utility.Util;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 */
public class SparkSimilaritiesNoCache {
	
	
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

	public static class ReadFiles implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<String, String>>, Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> {
		public Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> call(
				Tuple2<Tuple2<Integer, Integer>, Tuple2<String, String>> t) throws Exception {
			Tuple2<String, String> fns = t._2();
			FloatProcessorInfo fp1 = new FloatProcessorInfo(new ImagePlus(fns._1()).getProcessor().convertToFloatProcessor());
			FloatProcessorInfo fp2 = new FloatProcessorInfo(new ImagePlus(fns._1()).getProcessor().convertToFloatProcessor());
			return Util.tuple(t._1(), Util.tuple(fp1, fp2));
		}
	}

	public static class DownsampleAndIgnoreZeros implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>>, Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> {

		private final int sampleScale;

		public DownsampleAndIgnoreZeros(int sampleScale) {
			this.sampleScale = sampleScale;
		}

		public Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> call(
				Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> t) throws Exception {
			Tuple2<FloatProcessorInfo, FloatProcessorInfo> fps = t._2();
			FloatProcessor fp1 = (FloatProcessor) Downsampler.downsampleImageProcessor(fps._1().toFloatProcessor(), sampleScale);
			FloatProcessor fp2 = (FloatProcessor) Downsampler.downsampleImageProcessor(fps._2().toFloatProcessor(), sampleScale);
			FloatProcessorInfo fpi1 = new FloatProcessorInfo(fp1);
			FloatProcessorInfo fpi2 = new FloatProcessorInfo(fp2);
			float[] p1 = fpi1.getPixels();
			float[] p2 = fpi2.getPixels();
			for (int i = 0; i < p1.length; ++i) {
				if (p1[i] == 0.0)
					p1[i] = Float.NaN;

				if (p2[i] == 0.0)
					p2[i] = Float.NaN;
			}
			return Util.tuple(t._1(), Util.tuple(fpi1, fpi2));
		}
	}

	public static class Similarity implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>>, Tuple2<Integer, Integer>, Double> {
		public Tuple2<Tuple2<Integer, Integer>, Double> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> t) throws Exception {
			Tuple2<FloatProcessorInfo, FloatProcessorInfo> fps = t._2();
			return Util.tuple(t._1(), NCC.calculate(fps._1().toFloatProcessor(), fps._2().toFloatProcessor()));
		}
	}
	
	public static void run(String[] args) throws IOException {
		
		Options o = new Options(args);
		if ( o.isParsedSuccessfully() ) {

//			Class[] classesToBeRegistered = {
//					FloatProcessorInfo.class,
//			};
//			String classesToBeRegisteredString = classesToBeRegistered.length > 0 ? classesToBeRegistered[0].toString() : "";
//
//			for ( int i = 1; i < classesToBeRegistered.length; ++i )
//				classesToBeRegisteredString += "," + classesToBeRegistered[i].toString();
		
			final SparkConf conf      = new SparkConf()
					.setAppName("Similarity Matrix calculation")
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					// Now it's 24 Mb of buffer by default instead of 0.064 Mb
//					.set("spark.kryo.registrationRequired", "true")
					.set("spark.kryoserializer.buffer.mb","128")
					.set("spark.kryo.classesToRegister", "scala.collection.mutable.WrappedArray$ofRef" +
							"org.janelia.workshop.spark.utility.FloatProcessorInfo")
					;
	        final JavaSparkContext sc = new JavaSparkContext(conf);

//			conf.registerKryoClasses( classesToBeRegistered );
	        
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
	        
	        ArrayList< Tuple2< Tuple2< Integer, Integer >, Tuple2< String, String > > > filenamePairsWithIndexPairs =
					new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<String, String>>>();
	        for ( int i = 0;  i < size; ++i )
				for ( int k = i + 1; k < size && k - i <= range; ++k ) {
					System.out.println( i + " " + k + " " + size + " " + range + " " + filenames.size() );
					filenamePairsWithIndexPairs.add(Util.tuple(Util.tuple(i, k), Util.tuple(filenames.get(i), filenames.get(k))));//new Tuple2<Integer, String>( i, filenames.get( i ) ) );
				}

	        System.out.println( String.format( "Processing %d filenames with range=%d and defaultParallelism=%d.", filenames.size(), range, sc.defaultParallelism() ) );

			JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> filePairs = sc
					.parallelizePairs(filenamePairsWithIndexPairs)
					.mapToPair( new ReadFiles() )
					.mapToPair( new DownsampleAndIgnoreZeros( sampleScale ) );

			JavaPairRDD<Tuple2<Integer, Integer>, Double> similarities = filePairs.mapToPair(new Similarity() );
			
        
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

	public static void main(String[] args) throws IOException {
		run(args);

	}

}
