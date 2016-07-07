/**
 * 
 */
package org.janelia.workshop.spark.utility;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.io.Serializable;

/**
 * @author hanslovskyp
 *
 */
public class FloatProcessorInfo implements Serializable, KryoSerializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8418912980027228312L;
	
	private int width;
	private int height;
	private float[] pixels;
	
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

	public void write(Kryo kryo, Output output) {
		output.writeInt(width);
		output.writeInt(height);
		for ( float p : pixels )
			output.writeFloat( p );
	}

	public void read(Kryo kryo, Input input) {
		width = input.readInt();
		height = input.readInt();
		pixels = new float[width * height ];
		for ( int i = 0; i < pixels.length; ++i )
			pixels[i] = input.readFloat();
	}

	public static void main(String[] args) {

	}
}
