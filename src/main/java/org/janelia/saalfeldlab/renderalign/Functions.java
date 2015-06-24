package org.janelia.saalfeldlab.renderalign;

import org.apache.spark.api.java.function.Function2;

/**
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 */
public class Functions {

    public static class MergeCount implements Function2< Integer, Integer, Integer > {

		private static final long serialVersionUID = 7325360514874093953L;

		@Override
        public Integer call(final Integer count1, final Integer count2) throws Exception {
			return count1 + count2;
		}
	}
}
