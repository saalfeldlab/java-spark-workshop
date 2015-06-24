/**
 *
 */
package org.janelia.saalfeldlab.renderalign.util;

import ij.process.FloatProcessor;

import java.io.Serializable;

import net.imglib2.util.RealSum;

/**
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 *
 */
public class NCC implements Serializable {

	public static double byDouble(final float[] ap, final float[] bp) {
		double sumA = 0;
		double sumAA = 0;
		double sumB = 0;
		double sumBB = 0;
		double sumAB = 0;
		int n = 0;
		for (int i = 0; i < ap.length; ++i) {
			final float va = ap[i];
			final float vb = bp[i];

			if (Float.isNaN(va) || Float.isNaN(vb))
				continue;
			++n;
			sumA += va;
			sumAA += va * va;
			sumB += vb;
			sumBB += vb * vb;
			sumAB += va * vb;
		}
		return (n * sumAB - sumA * sumB) / Math.sqrt(n * sumAA - sumA * sumA) / Math.sqrt(n * sumBB - sumB * sumB);
	}

	public static double byDouble(final double[] ap, final double[] bp) {
		double sumA = 0;
		double sumAA = 0;
		double sumB = 0;
		double sumBB = 0;
		double sumAB = 0;
		int n = 0;
		for (int i = 0; i < ap.length; ++i) {
			final double va = ap[i];
			final double vb = bp[i];

			if (Double.isNaN(va) || Double.isNaN(vb))
				continue;
			++n;
			sumA += va;
			sumAA += va * va;
			sumB += vb;
			sumBB += vb * vb;
			sumAB += va * vb;
		}
		return (n * sumAB - sumA * sumB) / Math.sqrt(n * sumAA - sumA * sumA) / Math.sqrt(n * sumBB - sumB * sumB);
	}

	public static double byRealSym(final float[] ap, final float[] bp) {
		final RealSum sumA = new RealSum();
		final RealSum sumAA = new RealSum();
		final RealSum sumB = new RealSum();
		final RealSum sumBB = new RealSum();
		final RealSum sumAB = new RealSum();
		int n = 0;
		for (int i = 0; i < ap.length; ++i) {
			final float va = ap[i];
			final float vb = bp[i];

			if (Float.isNaN(va) || Float.isNaN(vb))
				continue;
			++n;
			sumA.add(va);
			sumAA.add(va * va);
			sumB.add(vb);
			sumBB.add(vb * vb);
			sumAB.add(va * vb);
		}
		final double suma = sumA.getSum();
		final double sumaa = sumAA.getSum();
		final double sumb = sumB.getSum();
		final double sumbb = sumBB.getSum();
		final double sumab = sumAB.getSum();

		return (n * sumab - suma * sumb) / Math.sqrt(n * sumaa - suma * suma) / Math.sqrt(n * sumbb - sumb * sumb);
	}

	public static double byRealSym(final double[] ap, final double[] bp) {
		final RealSum sumA = new RealSum();
		final RealSum sumAA = new RealSum();
		final RealSum sumB = new RealSum();
		final RealSum sumBB = new RealSum();
		final RealSum sumAB = new RealSum();
		int n = 0;
		for (int i = 0; i < ap.length; ++i) {
			final double va = ap[i];
			final double vb = bp[i];

			if (Double.isNaN(va) || Double.isNaN(vb))
				continue;
			++n;
			sumA.add(va);
			sumAA.add(va * va);
			sumB.add(vb);
			sumBB.add(vb * vb);
			sumAB.add(va * vb);
		}
		final double suma = sumA.getSum();
		final double sumaa = sumAA.getSum();
		final double sumb = sumB.getSum();
		final double sumbb = sumBB.getSum();
		final double sumab = sumAB.getSum();

		return (n * sumab - suma * sumb) / Math.sqrt(n * sumaa - suma * suma) / Math.sqrt(n * sumbb - sumb * sumb);
	}

	public static double calculate(final FloatProcessor fp1, final FloatProcessor fp2) {
		final float[] ap = (float[]) fp1.getPixels();
		final float[] bp = (float[]) fp2.getPixels();
		final RealSum sumA = new RealSum();
		final RealSum sumAA = new RealSum();
		final RealSum sumB = new RealSum();
		final RealSum sumBB = new RealSum();
		final RealSum sumAB = new RealSum();
		int n = 0;
		for (int i = 0; i < ap.length; ++i) {
			final float va = ap[i];
			final float vb = bp[i];

			if (vb < 1.0e-8f || vb < 1.0e-8f || Float.isNaN(va) || Float.isNaN(vb))
				continue;
			++n;
			sumA.add(va);
			sumAA.add(va * va);
			sumB.add(vb);
			sumBB.add(vb * vb);
			sumAB.add(va * vb);
		}
		final double suma = sumA.getSum();
		final double sumaa = sumAA.getSum();
		final double sumb = sumB.getSum();
		final double sumbb = sumBB.getSum();
		final double sumab = sumAB.getSum();

		return (n * sumab - suma * sumb) / Math.sqrt(n * sumaa - suma * suma) / Math.sqrt(n * sumbb - sumb * sumb);
	}
}
