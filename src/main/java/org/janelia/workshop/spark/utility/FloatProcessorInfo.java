/**
 * 
 */
package org.janelia.workshop.spark.utility;

import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.io.Serializable;

/**
 * @author hanslovskyp
 *
 */
public class FloatProcessorInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8418912980027228312L;
	
	private final int width;
	private final int height;
	private final float[] pixels;
	
	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public float[] getPixels() {
		return pixels;
	}
	
	/**
	 * @param width
	 * @param height
	 * @param pixels
	 */
	public FloatProcessorInfo( final int width, final int height, final float[] pixels ) {
		super();
		this.width  = width;
		this.height = height;
		this.pixels = pixels;
	}
	
	public FloatProcessorInfo( final FloatProcessor fp ) {
		this( fp.getWidth(), fp.getHeight(), (float[])fp.getPixels() );
	}
	
	public FloatProcessorInfo( final ImageProcessor ip ) {
		this( ip.convertToFloatProcessor() );
	}
	
	public FloatProcessor toFloatProcessor() {
		return new FloatProcessor( width, height, pixels );
	}
	
}
