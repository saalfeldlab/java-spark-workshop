/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.align;

import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.janelia.alignment.Render;
import org.janelia.alignment.RenderParameters;

/**
 *
 *
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org> and John Bogovic
 */
public class Util {

    /**
     * get the squared length of (a - b).
     *
     * @param a
     * @param b
     */
    public static double squareDistance(final float[] a, final float[] b) {
        double squ_len = 0.0;
        for (int i = 0; i < a.length; ++i)
            squ_len += (a[i] - b[i]) * (a[i] - b[i]);
        return squ_len;
    }

    final static public void fillNoise(final FloatProcessor ip, final double min, final double max) {
        final double scale = max - min;
        final Random rnd = new Random();
        final int n = ip.getWidth() * ip.getHeight();
        for (int i = 0; i < n; ++i)
                ip.setf(i, (float) (rnd.nextDouble() * scale + min));
    }

    final static public void fillNoise(final ColorProcessor ip, final int min, final int max) {
        final int scale = max - min + 1;
        final Random rnd = new Random();
        final int n = ip.getWidth() * ip.getHeight();
        for (int i = 0; i < n; ++i) {
            final int alpha = ip.get(i) & 0xff000000;
            final int r = rnd.nextInt(scale) + min;
            final int g = rnd.nextInt(scale) + min;
            final int b = rnd.nextInt(scale) + min;
            ip.set(i, ((((r << 8) | g) << 8) | b) | alpha );
        }
    }

    final static public void fillNoise(final ImageProcessor ip, final int min, final int max) {
        final int scale = max - min + 1;
        final Random rnd = new Random();
        final int n = ip.getWidth() * ip.getHeight();
        for (int i = 0; i < n; ++i)
                ip.set(i, rnd.nextInt(scale) + min);
    }

    final static public ByteProcessor createMask( final ColorProcessor ip) {
        final ByteProcessor mask = new ByteProcessor(ip.getWidth(), ip.getHeight());
        final byte[] maskPixels = (byte[])mask.getPixels();
        final int[] ipPixels = (int[])ip.getPixels();
        for (int i = 0; i < maskPixels.length; ++i)
            maskPixels[i] = (byte)(ipPixels[i] >> 24);
        return mask;
    }

    final static public ColorProcessor renderColorProcessor( final RenderParameters p )
    {
        final ColorProcessor ip = new ColorProcessor(p.openTargetImage());
        Util.fillNoise(ip, 0, 255);
        final BufferedImage targetImage = ip.getBufferedImage();

        Render.render(
                p.getTileSpecs(),
                targetImage,
                p.getX(),
                p.getY(),
                p.getRes(p.getScale()),
                p.getScale(),
                p.isAreaOffset(),
                p.getNumberOfThreads(),
                p.skipInterpolation(),
                p.doFilter());

        return new ColorProcessor(targetImage);
    }

    final static public float[] doublesToFloats( final double[] doubles ) {
        final float[] floats = new float[doubles.length];
        for (int i = 0; i < doubles.length; ++i)
            floats[i] = (float)doubles[i];
        return floats;
    }

    /**
     * Finds the highest index i of the largest value less than or equal to y in a
     * sorted list including repeated elements.  If y is less than the first value,
     * i = -1.
     *
     * Implemented as bin-search.
     *
     * @param list sorted list (increasing)
     * @param y reference value
     *
     * @return max(i) such that list[i] <= y
     */
    final static protected <T extends Comparable<T>> int lastLEQIndexInSortedList(
            final List<T> list,
            final T y) {
        int min = -1;
        int max = list.size();
        int i = max >> 1;
        do {
            if (list.get(i).compareTo(y) > 0)
                max = i;
            else
                min = i;
            i = ((max - min) >> 1) + min;
        } while (i != min);
        return i;
    }

    final static public void main(final String... args) {
        final ArrayList<Double> list = new ArrayList<Double>();
        list.add(new Double(1));
        list.add(new Double(2));
        list.add(new Double(2));
        list.add(new Double(2));
        list.add(new Double(5));
        list.add(new Double(6));
        list.add(new Double(7));
        list.add(new Double(7));
        list.add(new Double(7));

        System.out.println(list);

        final Double reference = new Double(0.9);
        System.out.println(lastLEQIndexInSortedList(list, reference));
    }

    /**
     * Calculate the pixel to world coordinate in a mipmap.
     *
     * @param x
     * @param scaleIndex
     * @return
     */
	static public double mipmapPixelToWorld(final double x, final int scaleIndex) {
		final double scale = 1L << scaleIndex;
		return (x + 0.5) * scale - 0.5;
	}

	/**
     * Calculate the world to pixel coordinate in a mipmap.
     *
     * @param x
     * @param scaleIndex
     * @return
     */
	static public double worldToMipmapPixel(final double y, final int scaleIndex) {
		final double scale = 1L << scaleIndex;
		return (y + 0.5) / scale - 0.5;
	}
}
