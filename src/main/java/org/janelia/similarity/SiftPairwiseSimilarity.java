/**
 * 
 */
package org.janelia.similarity;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import mpicbg.ij.FeatureTransform;
import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.models.AffineModel2D;
import mpicbg.models.Model;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;

/**
 * Calculate pairwise similarity matrix for image stack. To that end, extract SIFT  features for pairwise sections and 
 * find inliers and outliers of matches under a given model. The ratio of inliers to outliers + inliers will determine similarity
 * in the interval [0,1].
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 */
public class SiftPairwiseSimilarity {
	
	/**
	 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
	 * Helper class that holds parameters for pairwise sift feature extraction and matching. All members
	 * are public for straight forward modification.
	 */
	public static class Param {
		public FloatArray2DSIFT.Param p;
		public Integer maxSteps;
		public Float rod;
		public Float maxEpsilon;
		public Float minInlierRatio;
		public Integer minNumInliers;
		public Integer nThreads;
		public Boolean showProgress;
		public Integer range;
	}
	
	/**
	 * @return Param object with default parameters. Modify public members according to your needs.
	 */
	public static Param generateDefaultParameters() {
		final Param p = new Param();
		final FloatArray2DSIFT.Param siftp = new FloatArray2DSIFT.Param();
		siftp.fdSize = 4;
		siftp.maxOctaveSize = 1024;
		siftp.minOctaveSize = 1000;
		
		p.p              = siftp;
		p.maxSteps       = 3;
		p.rod            = 0.92f;
		p.maxEpsilon     = 50f;
		p.minInlierRatio = 0.05f;
		p.minNumInliers  = 10;
		p.nThreads       = Runtime.getRuntime().availableProcessors();
		p.showProgress   = true;
		p.range          = 50;
		
		return p;
	}
	
	
	private final Param p;
	
	
	/**
	 * @param p Parameters for feature extraction and matching.
	 * Construct SiftPairwiseSimilarity with default parameters.
	 */
	public SiftPairwiseSimilarity() {
		this( generateDefaultParameters() );
	}
	
	
	/**
	 * @param p Parameters for feature extraction and matching.
	 * Construct SiftPairwiseSimilarity with parameters p.
	 */
	public SiftPairwiseSimilarity(final Param p) {
		super();
		this.p = p;
	}


	/**
	 * @param ijSIFT {@link SIFT} object for extracting SIFT features.
	 * @param ip ImageJ ImageProcessor from which SIFT features will be extracted.
	 * @return ArrayList< Feature > List of extracted features.
	 * Convenience function that wraps ijSIFT.extractFeatures.
	 */
	public static ArrayList< Feature > extract( final SIFT ijSIFT, final ImageProcessor ip ) {
		final ArrayList<Feature> features = new ArrayList< Feature >();
		ijSIFT.extractFeatures( ip, features );
		return features;
	}
	
	
	/**
	 * @param n Dimension of matrix (nxn).
	 * @param featuresList List of features. featuresList.size() == n
	 * @return matrix with 1.0 or 0.0 on diagonal, NaN elsewhere
	 * Convenience function for creating a nxn matrix filled with NaNs, except for the diagonal where
	 * values are 1.0 if there are SIFT features for that section, and 0.0 otherwise.
	 */
	public static FloatProcessor generateMatrix( final int n, final ArrayList< List< Feature > > featuresList ) {
		final FloatProcessor matrix = new FloatProcessor( n, n );
		matrix.add( Double.NaN );
		for ( int i = 0; i < n; ++i ) {
			if ( featuresList.get( i ).size() > 0 )
				matrix.setf( i, i, 1.0f );
			else
				matrix.setf( i, i, 0.0f );
		}
		matrix.setMinAndMax( 0.0, 1.0 );
		return matrix;
	}
	
	
	/**
	 * @param model {@link Model} under which SIFT features should match, e.g. {@link AffineModel2D
	 * @param features1 First set of features for matching.
	 * @param features2 Second set of features for matching.
	 * @return Similarity based on ratio of inliers to outliers + inliers
	 * Determine similarity by matching two set of features and calculating the ratio of inliers to outliers + inliers.
	 */
	public < M extends Model< M > > double match( final M model, final List< Feature > features1, final List< Feature > features2 ) {
		
		final ArrayList<PointMatch> candidates = new ArrayList< PointMatch >();
		final ArrayList<PointMatch> inliers = new ArrayList< PointMatch >();
		double inlierRatio = 0.0;
	
		// can only fit model if features exist for both sections, return 0.0 otherwise
		if ( features1.size() > 0 && features2.size() > 0 ) {
			FeatureTransform.matchFeatures( features1, features2, candidates, p.rod );
			
			boolean modelFound = false;
			try {
				modelFound = model.filterRansac(
					candidates,
					inliers,
					1000,
					p.maxEpsilon,
					p.minInlierRatio,
					p.minNumInliers,
					3);
			}
			catch (final NotEnoughDataPointsException e) {
				modelFound = false;
			}
		
			// return 0.0, if model could not be fit to data 
			if (modelFound)
				inlierRatio = (double)inliers.size() / candidates.size();
		}
		
		return inlierRatio;
	}
	
	
	/**
	 * @param imp {@link ImagePlus} containing the stack for which SIFT features are to be extracted.
	 * @return List of features for each section of imp.
	 * Extract in parallel for each section of imp SIFT features, using SIFT parameters as specified in
	 * {@link Param.p}.
	 */
	public ArrayList< List< Feature > > extractFeatures( final ImagePlus imp ) {
		final ImageStack stack = imp.getStack();
		final int n = stack.getSize();
		final ArrayList< List< Feature > > featuresList = new ArrayList< List < Feature > >( n );
		// add null for each section, so featuresList will have an entry for each section before loop starts
		for ( int k = 0; k < n; ++k )
			featuresList.add( null );
		// Use atomic integer, so for no section features will be extracted twice.
		final AtomicInteger i = new AtomicInteger(0);
		final ArrayList<Thread> threads = new ArrayList< Thread >();
		for (int t = 0; t < p.nThreads; ++t) {
			final Thread thread = new Thread(
				new Runnable(){
					@Override
					public void run(){
						final FloatArray2DSIFT sift = new FloatArray2DSIFT( p.p );
						final SIFT ijSIFT = new SIFT(sift);
						// While there still are sections, ( k < n ), extract SIFT features.
						// After each iteration, go to next unprocessed section (k = i.getAndIncrement()).
						for (int k = i.getAndIncrement(); k < n; k = i.getAndIncrement()) {
							final ArrayList< Feature > features = extract( ijSIFT, stack.getProcessor(k + 1) );
							IJ.log( k + ": " + features.size() + " features extracted" );
							featuresList.set( k, features );
						}
					}
				}
			);
			threads.add(thread);
			thread.start();
		}
		// Wait until all threads are finished.
		for (final Thread t : threads)
			try {
				t.join();
			} catch (final InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return featuresList;
	}
	
	
	/**
	 * @param featuresList List of features for each section.
	 * @param model {@link Model} for transforming feature matches.
	 * @return ImagePlus containing the pairwise similarity matrix.
	 * Fit {@link Model} model to {@linkt PointMatch}es derived from featuresList and calculate similarity
	 * matrix as the ratio of inliers and inliers + outliers
	 */
	public < M extends Model< M > > ImagePlus matchFeaturesAndCalculateSimilarities( 
			final ArrayList< List< Feature > > featuresList,
			final M model) {
		final ArrayList<Thread> threads = new ArrayList< Thread >();
		final int n = featuresList.size(); // dimension of matrix = number of sections in stack
		final FloatProcessor matrix = generateMatrix( n, featuresList );
		final ImagePlus impMatrix = new ImagePlus("inlier ratio matrix", matrix);
		// show result while matrix is being filled with values
		if ( p.showProgress )
			impMatrix.show();
		// loop through all sections and compare each section to p.range next sections
		// k >= fi + 1 > i at all times
		for (int i = 0; i < n; ++i) {
			final int fi = i; // need to create final variable for use in Runnable.run()
			final List<Feature> f1 = featuresList.get( fi );
			final AtomicInteger j = new AtomicInteger(fi + 1);
			for (int t = 0; t < p.nThreads; ++t) {
				final Thread thread = new Thread(
					new Runnable(){
						@Override
						public void run(){
							for (int k = j.getAndIncrement(); k < n && k < fi + p.range; k = j.getAndIncrement()) {
								final List<Feature> f2 = featuresList.get( k );
								// get inlier ratio
								final float inlierRatio = (float)match( model, f1, f2 );
								matrix.setf(fi, k, inlierRatio);
								matrix.setf(k, fi, inlierRatio);
								impMatrix.updateAndDraw();
							}
						}
					}
				);
				threads.add(thread);
				thread.start();
			}
			for (final Thread t : threads)
				try {
					t.join();
				} catch (final InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return impMatrix;
	}
	
	
	/**
	 * @param imp {@link ImagePlus} containing the stack for which pairwise similarity matrix will be calculated.
	 * @param model {@link Model} for fitting {@link PointMatch}es and determining inliers and outliers. 
	 * @return {@link ImagePlus} of the similarity matrix.
	 * Glue function that puts together feature extraction and simliarity calculation. 
	 */
	public < M extends Model< M > > ImagePlus calculateSimilarityMatrix( final ImagePlus imp, final M model ) {
		final ArrayList<List<Feature>> featuresList = extractFeatures( imp );
		final ImagePlus impMatrix = matchFeaturesAndCalculateSimilarities( featuresList, model );
		return impMatrix;
	}
	
	
	public static void main(final String[] args) {
		
		final String filename = System.getProperty( "user.dir" ) + "/test_data_features.tif";
		final ImagePlus imp = new ImagePlus( filename );
		new ImageJ();
		imp.show();
		final FloatArray2DSIFT.Param siftp = new FloatArray2DSIFT.Param();
		siftp.fdSize = 4;
		siftp.maxOctaveSize = 1024;
		siftp.minOctaveSize = 64;
		final Param p = generateDefaultParameters();
		p.range        = 5;
		p.showProgress = true;
		p.p = siftp;
		final SiftPairwiseSimilarity SPS = new SiftPairwiseSimilarity( p );
		final AffineModel2D model = new AffineModel2D();
		final ImagePlus impMatrix = SPS.calculateSimilarityMatrix( imp, model );
	}
	
}
