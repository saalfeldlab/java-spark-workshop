/**
 * 
 */
package org.janelia.workshop.spark.utility;



import java.io.Serializable;

import ij.process.FloatProcessor;
import net.imglib2.util.RealSum;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 */
public class NCC implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4397691736499210953L;

	public static double calculate( FloatProcessor fp1, FloatProcessor fp2 ) {
		final float[] ap = (float[]) fp1.getPixels();
        final float[] bp = (float[]) fp2.getPixels();
        final RealSum sumA  = new RealSum();
        final RealSum sumAA = new RealSum();
        final RealSum sumB  = new RealSum();
        final RealSum sumBB = new RealSum();
        final RealSum sumAB = new RealSum();
        int n = 0;
        for (int i = 0; i < ap.length; ++i) {
            final float va = ap[i];
            final float vb = bp[i];

            if ( vb < 1.0e-8f || vb < 1.0e-8f || Float.isNaN( va ) || Float.isNaN( vb ) )
            	continue;
            ++n;
            sumA.add( va );
            sumAA.add( va * va);
            sumB.add( vb );
            sumBB.add( vb * vb);
            sumAB.add( va * vb );
        }
        final double suma = sumA.getSum();
        final double sumaa = sumAA.getSum();
        final double sumb = sumB.getSum();
        final double sumbb = sumBB.getSum();
        final double sumab = sumAB.getSum();

        return (n * sumab - suma * sumb) / Math.sqrt(n * sumaa - suma * suma) / Math.sqrt(n * sumbb - sumb * sumb);
	}
}
