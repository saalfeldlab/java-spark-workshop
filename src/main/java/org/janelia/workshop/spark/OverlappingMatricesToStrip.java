package org.janelia.workshop.spark;

import ij.ImageJ;
import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created by hanslovskyp on 6/10/16.
 */
public class OverlappingMatricesToStrip {

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

        @Option(name = "-r", aliases = {"--range"}, required = true,
                usage = "Correlation range. Must be smaller or equal than and defaults to <maximum-index> - <minimum-index>")
        private Integer range;

        @Option(name = "-s", aliases ={"--step"}, required = true,
                usage = ("step size"))
        private Integer step;

        @Option(name = "-O", aliases ={"--overlap"}, required = true,
                usage = ("Overlap of matrices"))
        private Integer overlap;

        @Option(name = "-v", aliases = {"--view"}, required = false,
                usage = ("View strip after completion" ) )
        private Boolean view = false;


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

        public Integer getStep() {
            return step;
        }

        public boolean isParsedSuccessfully() {
            return parsedSuccessfully;
        }

        public Integer getOverlap() {
            return overlap;
        }

        public Boolean doView() {
            return view;
        }

    }

    public static FloatProcessor run( Options o )
    {
//        Options o = new Options(args);
//        if ( o.isParsedSuccessfully() )
//        {
            final int start = o.getMinimum();
            final int stop  = o.getMaximum();
            final int range = o.getRange();
            final int step  = o.getStep();
            final int overlap = o.getOverlap();

            final String pattern = o.getFormat();

            FloatProcessor result = new FloatProcessor(2 * range + 1, stop - start);

            for ( int z = start; z < stop; z += step )
            {
                int lower = Math.max( z - overlap, start );
                int upper = Math.min( z + overlap + step, stop );
                System.out.println( z + " " + lower + " " + upper );

                FloatProcessor m = (FloatProcessor) new ImagePlus(String.format(pattern, lower, upper)).getProcessor();
                int w = m.getWidth();

                for ( int i = 0; i < w; ++i )
                {
                    for ( int r = -range; r <= range; ++r )
                    {
                        int k = i + r;
                        if ( k < 0 || k >= w )
                            continue;
                        result.setf( r + range, lower + i, m.getf( i, k ) );
                    }
                }

            }
            new FileSaver( new ImagePlus( "", result ) ).saveAsTiff( o.getOutput() );
            return result;
//        }
//        else
//            return null;
    }

    public static void main(String[] args) {
//        String argString = "-f /tier2/saalfeld/hanslovskyp/flyem/data/Z0115-22_Sec27/matrices/s=3.r=35/%d.%d.tif" +
//                " -o /tier2/saalfeld/hanslovskyp/flyem/data/Z0115-22_Sec27/matrices/strip.s=3.r=35.tif" +
//                " -m 0 -M 40766 -r 35 -s 140 -O 35 -v";
//        args = argString.split(" ");
        Options o = new Options( args );
        if ( o.isParsedSuccessfully() ) {
            FloatProcessor fp = run( o );
            if ( o.doView() ) {
                new ImageJ();
                new ImagePlus("", fp).show();
            }
        }
    }

}
