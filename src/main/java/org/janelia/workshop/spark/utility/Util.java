package org.janelia.workshop.spark.utility;

import scala.Tuple2;

/**
 * Created by phil on 6/6/16.
 */
public class Util {

    public static <T, U> Tuple2<T, U> tuple( T t, U u )
    {
        return new Tuple2< T, U >( t, u );
    }

}
