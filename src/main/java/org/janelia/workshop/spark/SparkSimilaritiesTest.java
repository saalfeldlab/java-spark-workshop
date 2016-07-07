package org.janelia.workshop.spark;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by hanslovskyp on 6/7/16.
 */
public class SparkSimilaritiesTest {
    public static void main(String[] args) throws IOException {
        System.setProperty( "spark.master", "local[*]" );
        System.setProperty( "spark.default.parallelism", "" + (3*Runtime.getRuntime().availableProcessors() ) );
        String pattern = "/tier2/flyem/data/Z0115-22_Sec27/align1/aligned/after.%05d.png";
        String outfile = "test-join.tif";
        String m = "0";
        String M = "55";
        String r = "25";
        String s = "2";
        String[] actualArgs = String.format("-f %s -o %s -m %s -M %s -r %s -s %s", pattern, outfile, m, M, r, s).split(" ");
        System.out.println(Arrays.toString(actualArgs));
        long t0 = System.currentTimeMillis();
        SparkSimilaritiesJoin.run(actualArgs);
        long t1 = System.currentTimeMillis();
        System.out.println("Runtime: " + (t1-t0) + "ms");
    }
}
