package org.janelia.workshop.spark;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by hanslovskyp on 6/7/16.
 */
public class SparkSimilaritiesNoCacheTest {
    public static void main(String[] args) throws IOException {
        System.setProperty( "spark.master", "local[*]" );
        String pattern = "/tier2/flyem/data/Z0115-22_Sec27/align1/aligned/after.%05d.png";
        String outfile = "test.tif";
        String m = "0";
        String M = "100";
        String r = "10";
        String s = "2";
        String[] actualArgs = String.format("-f %s -o %s -m %s -M %s -r %s -s %s", pattern, outfile, m, M, r, s).split(" ");
        System.out.println(Arrays.toString(actualArgs));
        long t0 = System.currentTimeMillis();
        SparkSimilaritiesNoCache.run(actualArgs);
        long t1 = System.currentTimeMillis();
        System.out.println("Runtime: " + (t1-t0) + "ms");
    }
}
