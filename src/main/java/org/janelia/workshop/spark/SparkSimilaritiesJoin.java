package org.janelia.workshop.spark;

        import ij.ImagePlus;
        import ij.io.FileSaver;
        import ij.process.FloatProcessor;

        import java.io.File;
        import java.io.IOException;
        import java.io.Serializable;
        import java.util.*;

        import org.apache.spark.HashPartitioner;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.broadcast.Broadcast;
        import org.janelia.workshop.spark.utility.FloatProcessorInfo;
        import org.janelia.workshop.spark.utility.Functions;
        import org.janelia.workshop.spark.utility.JoinFromList;
        import org.kohsuke.args4j.CmdLineException;
        import org.kohsuke.args4j.CmdLineParser;
        import org.kohsuke.args4j.Option;

        import scala.Tuple2;
        import scala.collection.mutable.WrappedArray$;
        import scala.reflect.ClassTag$;

/**
 * Created by hanslovskyp on 6/9/16.
 */
public class SparkSimilaritiesJoin {


    public static class Options {

        @Option(name = "-f", aliases = {"--format"}, required = true,
                usage = "Format string for input files.")
        private String format;

        @Option(name = "-o", aliases = {"--output"}, required = true,
                usage = "Output path for resulting matrix.")
        private String output;

        private final Integer defaultMinimum = 0;
        @Option(name = "-m", aliases = {"--minimum-index"}, required = false,
                usage = ("Minimum index for replacement in format string (inclusive), defaults to " + 0))
        private Integer minimum;

        @Option(name = "-M", aliases = {"--maximum-index"}, required = true,
                usage = "Maximum index for replacement in format string (exclusive).")
        private Integer maximum;

        @Option(name = "-r", aliases = {"--range"}, required = false,
                usage = "Correlation range. Must be smaller or equal than and defaults to <maximum-index> - <minimum-index>")
        private Integer range;

        @Option(name = "-s", aliases = {"--scale"}, required = false,
                usage = "Downscale images by a factor of 2^<scale> before calculating matrix. <scale> defaults to 0.")
        private Integer scale;

        @Option(name = "-F", aliases = {"--from-file"}, required = false,
                usage = "If specified, -f will be interpreted as path to file that contains all filenames instead of format string.")
        private Boolean fromFile = false;


        private boolean parsedSuccessfully = false;

        public Options(String[] args) {
            CmdLineParser parser = new CmdLineParser(this);
            try {
                parser.parseArgument(args);
                this.minimum = this.minimum == null ? this.defaultMinimum : this.minimum;
                final int maxRange = this.maximum - this.minimum - 1;
                this.range = this.range == null ? maxRange : this.range;

                if (this.range > maxRange)
                    throw new CmdLineException(parser, "--range cannot be larger than " + maxRange, new Exception());

                parsedSuccessfully = true;
            } catch (CmdLineException e) {
                System.err.println(e.getMessage());
                parser.printUsage(System.err);
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
        run(args);
    }


    public static void run(String[] args) throws IOException {

        Options o = new Options(args);
        if (o.isParsedSuccessfully()) {

//			Class[] classesToBeRegistered = {
//					FloatProcessorInfo.class
//			};
//			String classesToBeRegisteredString = classesToBeRegistered.length > 0 ? classesToBeRegistered[0].toString() : "";

//			for ( int i = 1; i < classesToBeRegistered.length; ++i )
//				classesToBeRegisteredString += "," + classesToBeRegistered[i].toString();


            final SparkConf conf = new SparkConf()
                    .setAppName("Similarity Matrix calculation")
                    .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
//                    .set("spark.kryo.registrationRequired", "true")
//                    .set("spark.kryoserializer.buffer", "1m")
//                    .set("spark.kryoserializer.buffer.max", "256m")
//                    .set( "spark.rdd.compress", "true" )
//                    .set("spark.kryo.classesToRegister", "scala.collection.mutable.WrappedArray$ofRef" +
//							",java.lang.Class.class" +
//							",scala.reflect.ClassTag$$anon$1" +
//                            ",org.janelia.workshop.spark.utility.FloatProcessorInfo") // classesToBeRegisteredString );
                    ;
//			conf.registerKryoClasses( classesToBeRegistered );
            final JavaSparkContext sc = new JavaSparkContext(conf);


//			conf.registerKryoClasses( classesToBeRegistered );
//
//			System.out.println( Arrays.toString( classesToBeRegistered ) );
//			System.out.println( classesToBeRegisteredString );

            conf.registerKryoClasses(new Class[]{
                    FloatProcessorInfo.class,
            });

            String[] keys = {
                    "spark.executor.memory",
                    "spark.eventLog.enabled",
                    "spark.serializer"
            };

            for (String k : keys)
            {
                try {
                    System.out.println(k + " " + conf.get(k));
                } catch (NoSuchElementException e) {
//                    e.printStackTrace();
                    System.out.println( k + " property does not exist." );
                }
            }

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

            HashMap<Integer, ArrayList<Integer>> indexPairs = new HashMap<Integer, ArrayList<Integer>>();

            ArrayList< Tuple2< Integer, String > > filenamesWithIndices = new ArrayList< Tuple2< Integer, String > >();
            for ( int i = 0;  i < size; ++i ) {
                filenamesWithIndices.add(new Tuple2<Integer, String>(i, filenames.get(i)));
                ArrayList<Integer> al = new ArrayList<Integer>();
                for ( int k = i + 1; k < size && k - i <= range; ++k )
                    al.add( k );
                indexPairs.put( i, al );
            }

            HashPartitioner p = new HashPartitioner(sc.defaultParallelism());


            System.out.println( String.format( "Processing %d filenames with range=%d and defaultParallelism=%d.", filenames.size(), range, sc.defaultParallelism() ) );

            int numberOfInitialPartitions = sc.defaultParallelism() / range;

            System.out.println( "Number of initial partitions: " + numberOfInitialPartitions );

            long t0 = System.currentTimeMillis();
            final JavaPairRDD<Integer, String> filenamesRDD            = sc.parallelizePairs( filenamesWithIndices, numberOfInitialPartitions ).sortByKey();//.sortByKey();
            final JavaPairRDD<Integer, FloatProcessorInfo> filesRDD    = filenamesRDD.mapToPair( new Functions.LoadFile( ) )
                    .mapToPair( new Functions.ReplaceValue( 0.0f, Float.NaN ) )
                    .mapToPair( new Functions.DownSample( sampleScale ) )
//                    .partitionBy( p )
                    .cache();
            long fileCount = filesRDD.count();
            long t1 = System.currentTimeMillis();
            long loadFileTime = t1 - t0;
            Tuple2<Integer, FloatProcessorInfo> first = filesRDD.first();
            System.out.println( "Files count: " + fileCount + " " + first._2().getWidth() + "x" + first._2().getHeight() );
//            for ( Tuple2<Integer, FloatProcessorInfo> fr : filesRDD.collect() ) {
//                System.out.println( fr );
//            }

            Broadcast<HashMap<Integer, ArrayList<Integer>>> bc = sc.broadcast(indexPairs);

            t0 = System.currentTimeMillis();
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> pairs =
                    JoinFromList.projectOntoSelf(sc, filesRDD, bc)
//                            .repartition(sc.defaultParallelism())
                            .cache();
            System.out.println( "Pairs count: " + pairs.count() );
            t1 = System.currentTimeMillis();
            long joinTime = t1 - t0;

            t0 = System.currentTimeMillis();
            JavaPairRDD<Tuple2<Integer, Integer>, Double> similarities = pairs.mapToPair(new SparkSimilaritiesNoCache.Similarity() ).cache();
            System.out.println( "Number of similarities partitions: " + similarities.getNumPartitions() );
            List<Tuple2<Tuple2<Integer, Integer>, Double>> values = similarities.collect();
            t1 = System.currentTimeMillis();
            long similaritiesTime = t1 - t0;

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

            System.out.println( String.format( "%-50s% 20dms", "Image pair loading time:", loadFileTime ) );
            System.out.println( String.format( "%-50s% 20dms", "Join time:", joinTime ) );
            System.out.println( String.format( "%-50s% 20dms", "Similarity calculation time:", similaritiesTime ) );

            sc.close();

        }
    }
}
