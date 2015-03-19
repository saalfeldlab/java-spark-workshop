package org.janelia.flintstone;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class Example {
	
	public static void runSparkJob( JavaSparkContext sc, int maxCount ) {
		
		ArrayList<Integer> numbers = new ArrayList< Integer >();
		for ( int i = 1; i <= maxCount; ++i ) numbers.add( i );
		
		Integer sum = sc.parallelize( numbers )
				.reduce( new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;
					public Integer call(Integer v1, Integer v2) throws Exception {
						return new Integer( v1.intValue() + v2.intValue() );
					}
				} )
				;
		
		System.out.println( String.format( "The sum of all integer values in the interval [%d..%d] is %d.", 1, maxCount, sum ) );
	}
	
	public static void main(String[] args) {
		int maxCount        = Integer.parseInt( args[0] );
		SparkConf conf      = new SparkConf().setAppName( "Example!" );
		JavaSparkContext sc = new JavaSparkContext( conf );
		runSparkJob( sc, maxCount );
		sc.close();
	}

}
