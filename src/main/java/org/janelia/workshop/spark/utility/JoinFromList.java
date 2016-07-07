package org.janelia.workshop.spark.utility;

/**
 * Created by hanslovskyp on 6/9/16.
 */


import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hanslovskyp on 9/24/15.
 */
public class JoinFromList {


    public static JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> projectOntoSelf(
            JavaSparkContext sc,
            JavaPairRDD<Integer, FloatProcessorInfo> rdd,
            Broadcast<HashMap< Integer, ArrayList< Integer >>> keyPairList )
    {

        JavaPairRDD<Tuple2<Integer, FloatProcessorInfo>, ArrayList<Integer>> keysImagesListOfKeys = rdd
                .mapToPair(new AssociateWith(keyPairList))
                .mapToPair(new MoveToKey())
                ;
        JavaPairRDD<Integer, Tuple2<Integer, FloatProcessorInfo>> flat = keysImagesListOfKeys
                .flatMapToPair( new FlatOut() )
                .mapToPair(new SwapKeyValue())
//                .partitionBy( rdd.partitioner().get() )
//                .sortByKey()
//                .repartition( sc.defaultParallelism() )
                ;

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>> joint = flat
                .join( rdd )
                .mapToPair( new Rearrange() )
                ;

        return joint;
    }

    /**
     *
     * rdd in:  ( T -> U )
     * map M:   { T -> V1 }
     * rdd out: ( TU -> U,V1 )
     *
     */
    public static class AssociateWith
            implements PairFunction< Tuple2< Integer, FloatProcessorInfo >, Integer, Tuple2< FloatProcessorInfo, ArrayList< Integer > > >
    {

        private final Broadcast<HashMap<Integer, ArrayList<Integer>>> keysAndValues;

        public AssociateWith(Broadcast< HashMap< Integer, ArrayList< Integer > > > keysAndValues) {
            super();
            this.keysAndValues = keysAndValues;
        }

        private static final long serialVersionUID = 6199058917722338402L;

        public Tuple2< Integer, Tuple2< FloatProcessorInfo,ArrayList< Integer > > > call(Tuple2<Integer, FloatProcessorInfo> t) throws Exception {
            Integer key = t._1();
            return Util.tuple( key, Util.tuple(t._2(), keysAndValues.getValue().get(key)) );
        }
    }

    /**
     *
     * rdd in:  ( K1 -> V1,V2 )
     * rdd out: ( K1,V1 -> V2 )
     *
     */
    public static class MoveToKey implements
            PairFunction< Tuple2< Integer, Tuple2< FloatProcessorInfo, ArrayList< Integer > > >, Tuple2<Integer, FloatProcessorInfo >, ArrayList< Integer > >
    {

        @Override
        public Tuple2<Tuple2<Integer, FloatProcessorInfo>, ArrayList<Integer>> call(Tuple2<Integer, Tuple2<FloatProcessorInfo, ArrayList<Integer>>> t) throws Exception {
            Tuple2<FloatProcessorInfo, ArrayList<Integer>> valuePair = t._2();
            return Util.tuple( Util.tuple( t._1(), valuePair._1() ), valuePair._2() );
        }
    }

    /**
     *
     * rdd in:  ( T -> [U] )
     * rdd out: ( T -> U )
     *
     */
    public static class FlatOut implements PairFlatMapFunction<
            Tuple2< Tuple2< Integer, FloatProcessorInfo>, ArrayList< Integer > >,
            Tuple2< Integer, FloatProcessorInfo >, Integer >
    {

        class MyIterator implements Iterator<Tuple2<Tuple2< Integer, FloatProcessorInfo>, Integer>>
        {

            private final Iterator< Integer > it;
            public MyIterator(Iterator<Integer> it, Tuple2<Integer,FloatProcessorInfo> constant) {
                super();
                this.it = it;
                this.constant = constant;
            }

            private final Tuple2<Integer,FloatProcessorInfo> constant;


            public boolean hasNext() {
                return it.hasNext();
            }

            public Tuple2< Tuple2<Integer, FloatProcessorInfo>, Integer> next() {
                return Util.tuple( constant, it.next() );
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        }

        private static final long serialVersionUID = -6076962646695402045L;

        public Iterable<Tuple2< Tuple2< Integer, FloatProcessorInfo >, Integer> > call( final Tuple2< Tuple2< Integer, FloatProcessorInfo>, ArrayList< Integer > > t ) throws Exception {
            Iterable<Tuple2<Tuple2<Integer, FloatProcessorInfo>, Integer>> iterable = new Iterable<Tuple2<Tuple2<Integer,FloatProcessorInfo>, Integer>>() {
                public Iterator<Tuple2<Tuple2<Integer, FloatProcessorInfo>, Integer>> iterator() {
                    return new MyIterator( t._2().iterator(), t._1() );
                }

            };
            return iterable;
        }
    }

//    public static class RearrangeSameKeysAndValues< K, V > extends Rearrange< K, K, V, V > {}

    /**
     *
     * rdd in:  ( K1 -> (K2,V2),V2 )
     * rdd out: ( K1,K2 -> V1,V2 )
     *
     * @param <K1>
     * @param <V1>
     */
    public static class Rearrange implements
            PairFunction<
                    Tuple2<Integer,Tuple2<Tuple2<Integer, FloatProcessorInfo>, FloatProcessorInfo>>,
                    Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo> >
    {

        private static final long serialVersionUID = -5511873062115999278L;

        public Tuple2<Tuple2<Integer, Integer>, Tuple2<FloatProcessorInfo, FloatProcessorInfo>>
        call(Tuple2<Integer, Tuple2<Tuple2<Integer, FloatProcessorInfo>, FloatProcessorInfo>> t) throws Exception {
            Tuple2<Tuple2<Integer, FloatProcessorInfo>, FloatProcessorInfo> t2 = t._2();
            Tuple2<Integer, FloatProcessorInfo> t21 = t2._1();
            return Util.tuple( Util.tuple( t._1(), t21._1() ), Util.tuple( t2._2(), t21._2() ) );
        }
    }

    /**
     *
     * rdd in:  ( K -> V )
     * rdd out: ( V -> K )
     */
    public static class SwapKeyValue implements PairFunction<Tuple2<Tuple2<Integer, FloatProcessorInfo>, Integer>, Integer, Tuple2< Integer, FloatProcessorInfo > >
    {
        private static final long serialVersionUID = -4173593608656179460L;

        public Tuple2<Integer, Tuple2<Integer, FloatProcessorInfo>> call(Tuple2<Tuple2< Integer, FloatProcessorInfo>, Integer> t) throws Exception {
            return Util.tuple( t._2(), t._1() );
        }
    }

}
