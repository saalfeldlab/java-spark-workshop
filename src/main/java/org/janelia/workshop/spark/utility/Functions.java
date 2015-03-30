package org.janelia.workshop.spark.utility;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import mpicbg.trakem2.util.Downsampler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import scala.Tuple3;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 */
public class Functions {
	
	public static class ExtractFromCsv implements PairFunction< Tuple2< Integer, String >, Integer, Double > {
		
		private static final long serialVersionUID = -1589517586604057477L;
		private final String separator;
		
		public ExtractFromCsv() {
			this( ",", 0 );
		}
		
		/**
		 * @param separator
		 * @param column
		 */
		public ExtractFromCsv(String separator, int column) {
			super();
			this.separator = separator;
			this.column = column;
		}

		private final int column;

		public Tuple2<Integer, Double> call(Tuple2<Integer, String> indexedString ) throws Exception {
			return new Tuple2< Integer, Double >( 
					indexedString._1(),
					Double.parseDouble( indexedString._2().split( separator )[ column ] ) 
					);
		}
	}
	
	
	public static class Scale implements PairFunction< Tuple2< Integer, Double >, Integer, Double > {
		
		private static final long serialVersionUID = -5988331911722707396L;
		private final double scale;

		/**
		 * @param scale
		 */
		public Scale(double scale) {
			super();
			this.scale = scale;
		}

		public Tuple2<Integer, Double> call(Tuple2<Integer, Double> tuple )
				throws Exception {
			return new Tuple2< Integer, Double >( tuple._1(), tuple._2() * this.scale );
		}
		
	}
	
	
public static class Add implements PairFunction< Tuple2< Integer, Double >, Integer, Double > {
		
	private static final long serialVersionUID = 6350599396357700077L;
		private final double summand;

		/**
		 * @param scale
		 */
		public Add(double summand) {
			super();
			this.summand = summand;
		}

		public Tuple2<Integer, Double> call(Tuple2<Integer, Double> tuple )
				throws Exception {
			return new Tuple2< Integer, Double >( tuple._1(), tuple._2() + this.summand );
		}
		
	}
	
	
	public static class DoubleOnly< T > implements Function< Tuple2< T, Double>, Double > {

		private static final long serialVersionUID = 769007427304825373L;

		public Double call(Tuple2<T, Double> tuple) throws Exception {
			return tuple._2();
		}
		
	}
	
	public static class DoubleComparator implements Comparator<Double>, Serializable {
		private static final long serialVersionUID = -5352235412806822171L;

		public int compare(Double o1, Double o2) {
			return o1.doubleValue() < o2.doubleValue() ? -1 : ( o1.doubleValue() == o2.doubleValue() ? 0 : 1 );  
		}
	}
	
	public static class TransformIndexPairsToIndexPairWithWeight implements 
	PairFunction<Tuple2<Tuple2<Integer,Double>,Integer>, Integer, Tuple2< Integer, Double > > {
		
		private static final long serialVersionUID = 4157687854367326392L;
		private final double negativeInverseTwoSquareSigma;
		
		
		/**
		 * 
		 */
		public TransformIndexPairsToIndexPairWithWeight( final double sigma ) {
			super();
			this.negativeInverseTwoSquareSigma = -0.5 / ( sigma * sigma );
		}

		public double gaussOffset( double x, double mean ) {
			double diff = x - mean;
			return Math.exp( this.negativeInverseTwoSquareSigma * diff * diff );
		}

		public Tuple2< Integer, Tuple2<Integer,Double> > call(
				Tuple2<Tuple2<Integer, Double>, Integer> arg0) throws Exception {
			Tuple2<Integer, Double> tuple = arg0._1();
			Integer i1 = tuple._1();
			Integer i2 = arg0._2();
			Double mean = tuple._2();
			return new Tuple2< Integer, Tuple2< Integer, Double > >( i2, new Tuple2< Integer, Double> ( i1, gaussOffset( i2, mean ) ) );
		}
		
	}
	
	
	public static class CreateEmptyList< T > implements Function< T, ArrayList< T > > {

		private static final long serialVersionUID = -6576941021236749313L;
		private final ArrayList< T > dummy;

		/**
		 * @param dummy
		 */
		public CreateEmptyList(ArrayList<T> dummy) {
			super();
			this.dummy = dummy;
		}

		public ArrayList< T > call( T t )
				throws Exception {
			ArrayList< T > result = new ArrayList< T >( dummy );
			result.clear();
			result.add( t );
			return result;
		}
	}
	
	
	public static class AppendToList< T > implements Function2< ArrayList< T >, T, ArrayList< T > > {

		private static final long serialVersionUID = -8409634481249534903L;

		public ArrayList< T > call( ArrayList< T > arg0, T arg1 ) throws Exception {
			arg0.add( arg1 );
			return arg0;
		}
	}
	
	public static class MergeLists< T > implements Function2< ArrayList< T >, ArrayList< T >, ArrayList< T > > {

		private static final long serialVersionUID = -8409634481249534903L;

		public ArrayList< T > call( ArrayList< T > arg0, ArrayList< T > arg1 ) throws Exception {
			ArrayList<T> result = new ArrayList< T >(arg0);
			result.addAll( arg1 );
			return result;
		}
	}
	
	
	public static class Filter implements Function<Tuple2<Integer,Tuple2<Integer,Double> >, Boolean> {

		private static final long serialVersionUID = -3219803659743961444L;
		private final double threshold;

		/**
		 * @param threshold
		 */
		public Filter(double threshold) {
			super();
			this.threshold = threshold;
		}

		public Boolean call( Tuple2<Integer, Tuple2<Integer, Double>> tuple )
				throws Exception {
			return tuple._2()._2() > threshold;
		}
		
	}
	
	
	public static class LoadFile implements PairFunction< Tuple2< Integer, String >, Integer, FloatProcessorInfo > {
		
		private static final long serialVersionUID = -5220501390575963707L;

		public Tuple2< Integer, FloatProcessorInfo > call( Tuple2< Integer, String > tuple ) throws Exception {
			Integer index     = tuple._1();
			String filename   = tuple._2();
			FloatProcessor fp = new ImagePlus( filename ).getProcessor().convertToFloatProcessor();
			return new Tuple2< Integer, FloatProcessorInfo >( index, new FloatProcessorInfo( fp ) );
		}
	}
	
	
	public static class WriteToFormatStringAndCountOnSuccess
	implements Function2< Integer, Tuple2< Integer, FloatProcessorInfo >, Integer > {
		
		private static final long serialVersionUID = 6477905125934154458L;
		private final String format;

		/**
		 * @param format
		 */
		public WriteToFormatStringAndCountOnSuccess(String format) {
			super();
			this.format = format;
		}

		public Integer call(Integer count,
				Tuple2<Integer, FloatProcessorInfo> input) throws Exception {
//			System.out.println( input._1() + " " + input._2().getWidth() + " " + input._2().getHeight() );
			new FileSaver( new ImagePlus( "", input._2().toFloatProcessor().convertToByteProcessor( false ) ) ).saveAsTiff( String.format( this.format, input._1() ) );
			return count + 1;
		}
		
	}
	
	
	public static class MergeCount
	implements Function2< Integer, Integer, Integer > {

		private static final long serialVersionUID = 7325360514874093953L;

		public Integer call(Integer count1, Integer count2) throws Exception {
			return count1 + count2;
		}
	}
	
	
	public static class ReverseKey< T >
	implements PairFunction<Tuple2<Integer,Tuple2<Integer,T>>,  Integer, Tuple2< Integer, T > > {

		private static final long serialVersionUID = 6349537992646145197L;

		public Tuple2<Integer, Tuple2<Integer, T>> call(
				Tuple2<Integer, Tuple2<Integer, T>> t) throws Exception {
			Tuple2<Integer, T> oldTuple = t._2();
			return new Tuple2< Integer, Tuple2< Integer, T > >( oldTuple._1(), new Tuple2< Integer, T >( t._1(), oldTuple._2() ) );
		}
	}
	
	
	public static class CreateFloatProcessorInfoWithWeightSum
	implements Function<Tuple2<FloatProcessorInfo,Double>, Tuple2< FloatProcessorInfo, FloatProcessorInfo > > {

		private static final long serialVersionUID = 5669591460867970788L;

		public Tuple2< FloatProcessorInfo, FloatProcessorInfo > call( Tuple2<FloatProcessorInfo, Double> weightedFloatProcessorInfo )
				throws Exception {
			float weight           = weightedFloatProcessorInfo._2().floatValue();
			FloatProcessorInfo fpi = weightedFloatProcessorInfo._1();
			float[] pixels         = fpi.getPixels().clone();
			float[] weights        = new float[ pixels.length ];
			
			for (int i = 0; i < pixels.length; i++) {
				float val  = pixels[i];
				pixels[i]  = weight*val;
				weights[i] = val > 0.0f ? weight : val;
			}
			
			FloatProcessorInfo dataFpi    = new FloatProcessorInfo( fpi.getWidth(), fpi.getHeight(), pixels );
			FloatProcessorInfo weightsFpi = new FloatProcessorInfo( fpi.getWidth(), fpi.getHeight(), weights );
			
			return new Tuple2< FloatProcessorInfo, FloatProcessorInfo >( dataFpi, weightsFpi );
		}
	}
	
	
	public static class AddWeightedFloatProcessorInfoWithWeightSum
	implements Function2< Tuple2< FloatProcessorInfo, FloatProcessorInfo >, Tuple2<FloatProcessorInfo,Double>, Tuple2< FloatProcessorInfo, FloatProcessorInfo > > {

		private static final long serialVersionUID = 4371511331712281055L;

		public Tuple2< FloatProcessorInfo, FloatProcessorInfo > call(Tuple2< FloatProcessorInfo, FloatProcessorInfo > sumWithWeight,
				Tuple2<FloatProcessorInfo, Double> summand) throws Exception {
			FloatProcessorInfo fpi = summand._1();
			float weight           = summand._2().floatValue();
			float[] sumPixels      = sumWithWeight._1().getPixels();
			float[] weightPixels   = sumWithWeight._2().getPixels();
			float[] summandPixels  = fpi.getPixels();
			for (int i = 0; i < summandPixels.length; i++) {
				float val = summandPixels[i];
				if ( val > 0.0f ) {
					sumPixels[i]    += weight*val;
					weightPixels[i] += weight;
				}
			}
			return sumWithWeight;
		}
	}
	
	
	public static class MergeWeightedFloatProcessorInfoWithWeightSum
	implements Function2< Tuple2< FloatProcessorInfo, FloatProcessorInfo >, Tuple2< FloatProcessorInfo, FloatProcessorInfo >, Tuple2< FloatProcessorInfo, FloatProcessorInfo > > {

		private static final long serialVersionUID = 4371511331712281055L;

		public Tuple2< FloatProcessorInfo, FloatProcessorInfo > call(Tuple2< FloatProcessorInfo, FloatProcessorInfo > sumWithWeights1,
				Tuple2< FloatProcessorInfo, FloatProcessorInfo > sumWithWeights2) throws Exception {
			float[] sumPixels1    = sumWithWeights1._1().getPixels();
			float[] sumPixels2    = sumWithWeights2._1().getPixels();
			float[] weightPixels1 = sumWithWeights1._2().getPixels();
			float[] weightPixels2 = sumWithWeights2._2().getPixels();
			float[] resSum        = sumPixels1.clone();
			float[] resWeights    = weightPixels1.clone();
			for (int i = 0; i < sumPixels1.length; i++) {
				resSum[i]     += sumPixels2[i];
				resWeights[i] += weightPixels2[i];
			}
			
			FloatProcessorInfo resSumFpi    = new FloatProcessorInfo( sumWithWeights1._1().getWidth(), sumWithWeights1._1().getHeight(), resSum );
			FloatProcessorInfo resWeightFpi = new FloatProcessorInfo( sumWithWeights1._1().getWidth(), sumWithWeights1._1().getHeight(), resWeights );
			
			return new Tuple2< FloatProcessorInfo, FloatProcessorInfo >( resSumFpi, resWeightFpi );
		}
	}
	
	
	public static class NormalizeFloatProcessorInfo
	implements PairFunction<Tuple2<Integer,Tuple2<FloatProcessorInfo,FloatProcessorInfo>>, Integer, FloatProcessorInfo > {

		private static final long serialVersionUID = 6785642860370059493L;

		public Tuple2<Integer, FloatProcessorInfo> call(
				Tuple2<Integer, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> t)
				throws Exception {
			float[] data = t._2()._1().getPixels();
			float[] weights = t._2()._2().getPixels();
			for (int i = 0; i < weights.length; ++i )
				data[i] /= weights[i];
			return new Tuple2< Integer, FloatProcessorInfo >( t._1(), t._2()._1() );
		}
	}
	
	
	public static class FlattenFloatProcessorInfoWithTargetList< T, U >
	implements FlatMapFunction<Tuple2<FloatProcessorInfo,ArrayList<Tuple2< T,U>>>, Tuple3< T, FloatProcessorInfo, U > > {

		private static final long serialVersionUID = 8067247458902129880L;

		public Iterable<Tuple3<T, FloatProcessorInfo, U>> call(
				Tuple2<FloatProcessorInfo, ArrayList<Tuple2<T, U>>> t)
				throws Exception {
			ArrayList<Tuple3<T, FloatProcessorInfo, U>> res = new ArrayList< Tuple3< T, FloatProcessorInfo, U > >();
			FloatProcessorInfo fpi = t._1();
			for ( Tuple2<T, U> pair : t._2() )
				res.add( new Tuple3< T, FloatProcessorInfo, U >( pair._1(), fpi, pair._2() ) );
			return res;
		}
	}
	
	
	public static class TripleToKeyPair< T, U, V>
	implements PairFunction< Tuple3< T, U, V >, T, Tuple2< U, V > > {

		private static final long serialVersionUID = -5220575730668915704L;

		public Tuple2<T, Tuple2<U, V>> call(Tuple3<T, U, V> t) throws Exception {
			return new Tuple2< T, Tuple2< U, V > >( t._1(), new Tuple2< U, V >( t._2(), t._3() ) );
		}
	}
	
	
	public static class CombineByKeyWithBroadcast< T, U >
	implements Function<Tuple2<Integer,T>, Tuple2< T, U > > {
		
		private static final long serialVersionUID = -2310514473874600020L;
		private final Broadcast< List< Tuple2< Integer, U > > > broadcast;


		/**
		 * @param broadcast
		 */
		public CombineByKeyWithBroadcast(
				Broadcast<List<Tuple2<Integer, U>>> broadcast) {
			super();
			this.broadcast = broadcast;
		}


		public Tuple2<T, U > call(Tuple2<Integer, T> tuple) throws Exception {
			List<Tuple2<Integer, U>> list = broadcast.getValue();
			int index = tuple._1().intValue();
			T t = tuple._2();
			U u = null;
			//			List< Tuple2<Integer, Double> > val = null;
			for ( Tuple2<Integer, U> l : list ) {
				if ( l._1().intValue() == index ) {
					u  = l._2();
				}
			}
			if ( u == null ) throw new NullPointerException( "Image index not found in weighted source to target mapping!" );
			return new Tuple2< T, U>( t, u );
		}
	}
	
	
	public static class ReplaceValue implements PairFunction< Tuple2< Integer, FloatProcessorInfo >, Integer, FloatProcessorInfo > {
		
		private final float value;
		private final float replacement;

		/**
		 * @param value
		 * @param replacement
		 */
		public ReplaceValue(float value, float replacement) {
			super();
			this.value = value;
			this.replacement = replacement;
		}

		private static final long serialVersionUID = 5642948550521110112L;

		public Tuple2<Integer, FloatProcessorInfo> call(
				Tuple2<Integer, FloatProcessorInfo> indexedFloatProcessor ) throws Exception {
			FloatProcessor fp = (FloatProcessor)indexedFloatProcessor._2().toFloatProcessor().clone();
			float[] pixels = (float[])fp.getPixels();
			for ( int i = 0; i < pixels.length; ++i )
				if ( pixels[i] == this.value )
					pixels[i] = this.replacement;
			return new Tuple2< Integer, FloatProcessorInfo >( indexedFloatProcessor._1(), new FloatProcessorInfo( fp ) );
		}
	}
	
	
	public static class DownSample implements PairFunction< Tuple2< Integer, FloatProcessorInfo >, Integer, FloatProcessorInfo > {
		
		private final int sampleScale;

		/**
		 * @param sampleScale
		 */
		public DownSample(int sampleScale) {
			super();
			this.sampleScale = sampleScale;
		}

		private static final long serialVersionUID = -8634964011671381854L;

		public Tuple2<Integer, FloatProcessorInfo> call(
				Tuple2<Integer, FloatProcessorInfo> indexedFloatProcessor ) throws Exception {
			FloatProcessor downsampled = (FloatProcessor)Downsampler.downsampleImageProcessor( indexedFloatProcessor._2().toFloatProcessor(), sampleScale );
			return new Tuple2< Integer, FloatProcessorInfo >( indexedFloatProcessor._1(), new FloatProcessorInfo( downsampled ) );
		}
	}


	public static class RangeFilter implements Function< Tuple2 < Tuple2< Integer,FloatProcessorInfo >,Tuple2<Integer,FloatProcessorInfo> >, Boolean > {
		
		private final int range;
	
		/**
		 * @param range
		 */
		public RangeFilter(int range) {
			super();
			this.range = range;
		}
	
		private static final long serialVersionUID = 7657057842779788590L;
	
		public Boolean call(
				Tuple2< Tuple2< Integer, FloatProcessorInfo>, Tuple2<Integer, FloatProcessorInfo> > tuples )
				throws Exception {
			Integer index1 = tuples._1()._1();
			Integer index2 = tuples._2()._1();
			if ( index2 > index1 && index2 - index1 < range )
				return true;
			else
				return false;
		}
	}
	
	
	public static class PairwiseSimilarity implements 
	PairFunction<Tuple2<Tuple2<Integer,FloatProcessorInfo>,Tuple2<Integer,FloatProcessorInfo>>, Tuple2< Integer, Integer >, Double > {
		
		private static final long serialVersionUID = -8120254940086227756L;

		public Tuple2< Tuple2< Integer, Integer >, Double > call(
				Tuple2<Tuple2<Integer, FloatProcessorInfo>, Tuple2<Integer, FloatProcessorInfo>> tuples)
				throws Exception {
			Tuple2<Integer, FloatProcessorInfo> t1 = tuples._1();
			Tuple2<Integer, FloatProcessorInfo> t2 = tuples._2();
			Tuple2<Integer, Integer> indexTuple    = new Tuple2< Integer, Integer >( t1._1(), t2._1() );
			double similarity = NCC.calculate( t1._2().toFloatProcessor(), t2._2().toFloatProcessor() );
			return new Tuple2< Tuple2< Integer, Integer >, Double >( indexTuple, similarity );
		}
		
	}
	
}
