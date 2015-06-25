/**
 *
 */
package org.janelia.saalfeldlab.renderalign;

import ij.IJ;
import ij.ImageJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.FloatProcessor;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import mpicbg.ij.FeatureTransform;
import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.models.AffineModel2D;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.alignment.Render;
import org.janelia.alignment.RenderParameters;
import org.janelia.alignment.json.JsonUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 *
 */
public class SimilarityRenderMontages {

	public static class Options implements Serializable {

		@Option(name = "-S", aliases = {"--server"}, required = true, usage = "Server base URL.")
		private String server = "http://tem-services.int.janelia.org:8080/render-ws/v1";

		@Option(name = "-u", aliases = {"--owner", "--user"}, required = true, usage = "Owner.")
		private String owner = "flyTEM";

		@Option(name = "-p", aliases = {"--project"}, required = true, usage = "Project ID.")
		private String projectId = "FAFB00";

		@Option(name = "-s", aliases = {"--stack"}, required = true, usage = "Stack ID.")
		private String stackId = "v5_align_tps";

		@Option(name = "-x", aliases = {"--x"}, required = true, usage = "Left most pixel coordinate in world coordinates.")
		private Double x = 100000.0;

		@Option(name = "-y", aliases = {"--y"}, required = true, usage = "Top most pixel coordinate in world coordinates.")
		private Double y = 60000.0;

		@Option(name = "-w", aliases = {"--width"}, required = true, usage = "Width in world coordinates.")
		private Double w = 2048.0 * 8.0;

		@Option(name = "-h", aliases = {"--height"}, required = true, usage = "Height in world coordinates.")
		private Double h = 2048.0 * 8.0;

		@Option(name = "-t", aliases = {"--scale"}, required = true, usage = "Scale.")
		private Double scale = 1.0 / 8.0;

	    private boolean parsedSuccessfully = false;

		public Options(final String[] args) {
			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
			}
		}

		public String getServer() {
			return server;
		}

		public String getOwner() {
			return owner;
		}

		public String getProjectId() {
			return projectId;
		}

		public String getStackId() {
			return stackId;
		}

		public Double getX() {
			return x;
		}

		public Double getY() {
			return y;
		}

		public Double getW() {
			return w;
		}

		public Double getH() {
			return h;
		}

		public Double getScale() {
			return scale;
		}

		public boolean isParsedSuccessfully() {
			return parsedSuccessfully;
		}
	}

	public static void main(final String... args) throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		final String baseUrlString = options.getServer() +
                "/owner/" + options.getOwner() +
                "/project/" + options.getProjectId() +
                "/stack/" + options.getStackId();

		final URL zValuesUrl = new URL(baseUrlString + "/zValues");
//		final ArrayList<Double> zs =
//		        JsonUtils.GSON.fromJson(
//		                new InputStreamReader(zValuesUrl.openStream()),
//		                new TypeToken<ArrayList<Double>>(){}.getType());

		final List<Double> zs = Arrays.asList(new Double[]{
				  3451.0,
				  3452.0,
				  3453.0,
				  3454.0,
				  3455.0,
				  3456.0,
				  3457.0,
				  3458.0,
				  3459.0,
				  3460.0,
				  3461.0,
				  3462.0,
				  3463.0,
				  3464.0,
				  3465.0,
				  3466.0,
				  3467.0
			});

		final String urlString =
				baseUrlString +
				"/z/%f/box/" + (int)Math.round(options.getX()) + "," + (int)Math.round(options.getY()) + "," + (int)Math.round(options.getW()) + "," + (int)Math.round(options.getH()) + "," + options.getScale() +
				"/render-parameters";

		final SparkConf conf      = new SparkConf().setAppName( "RenderSimilarities" );
        final JavaSparkContext sc = new JavaSparkContext(conf);

		final JavaRDD<Double> rddZ = sc.parallelize(zs);
		final JavaPairRDD<Double, String> urls = rddZ.mapToPair(
				new PairFunction<Double, Double, String>() {

					@Override
					public Tuple2<Double, String> call(final Double z) throws Exception {
						return new Tuple2<Double, String>(z, String.format(urlString, z));
					}
				});

		final JavaPairRDD<Double, RenderParameters> parameters = urls.mapToPair(
				new PairFunction<Tuple2<Double, String>, Double, RenderParameters>() {

					@Override
					public Tuple2<Double, RenderParameters> call(final Tuple2<Double, String> pair) throws Exception {
						return new Tuple2<Double, RenderParameters>(
								pair._1(),
								RenderParameters.parseJson(new InputStreamReader(new URL(pair._2()).openStream())));
					}
				});

		final JavaPairRDD<Double, BufferedImage> images = parameters.mapToPair(
				new PairFunction<Tuple2<Double, RenderParameters>, Double, BufferedImage>() {

					@Override
					public Tuple2<Double, BufferedImage> call(final Tuple2<Double, RenderParameters> pair) throws Exception {

						final RenderParameters param = pair._2();
						final BufferedImage targetImage = param.openTargetImage();
						final ByteProcessor ip = new ByteProcessor(targetImage.getWidth(), targetImage.getHeight());
						mpicbg.ij.util.Util.fillWithNoise(ip);
						targetImage.getGraphics().drawImage(ip.createImage(), 0, 0, null);

						try {
        					Render.render(
        							param.getTileSpecs(),
        							targetImage,
        							options.getX(),
        							options.getY(),
        							param.getRes(options.getScale()),
        							options.getScale(),
        							false,
        							1,
        							false,
        							true);
						} catch (final IndexOutOfBoundsException e) {
						    System.out.println("Failed rendering layer " + pair._1() + " because");
						    e.printStackTrace(System.out);
						}

						return new Tuple2<Double, BufferedImage>(
								pair._1(),
								targetImage);
					}
				});

		final JavaPairRDD<Double, ArrayList<Feature>> features = images.mapToPair(
				new PairFunction<Tuple2<Double, BufferedImage>, Double, ArrayList<Feature>>() {

					@Override
					public Tuple2<Double, ArrayList<Feature>> call(final Tuple2<Double, BufferedImage> pair) throws Exception {

						final BufferedImage img = pair._2();
						final ColorProcessor ip = new ColorProcessor(img);

						final FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param();

						p.fdSize = 4;
						p.maxOctaveSize = 4000;
						p.minOctaveSize = 1500;

						final FloatArray2DSIFT sift = new FloatArray2DSIFT(p);
						final SIFT ijSIFT = new SIFT(sift);

						final ArrayList<Feature> fs = new ArrayList<Feature>();
						ijSIFT.extractFeatures(ip, fs);
						return new Tuple2<Double, ArrayList<Feature>>(pair._1(), fs);
					}
				});

		features.cache();

		final JavaPairRDD<Tuple2<Double, ArrayList<Feature>>, Tuple2<Double, ArrayList<Feature>>> cartesian = features.cartesian(features);

		final JavaPairRDD<Tuple2<Double, Double>, Double> similarity = cartesian.mapToPair(
				new PairFunction<Tuple2<Tuple2<Double, ArrayList<Feature>>, Tuple2<Double, ArrayList<Feature>>>, Tuple2<Double, Double>, Double>() {

					@Override
					public Tuple2<Tuple2<Double, Double>, Double> call(final Tuple2<Tuple2<Double, ArrayList<Feature>>, Tuple2<Double, ArrayList<Feature>>> pair) {

						final float rod = 0.92f;
						final float maxEpsilon = 50f;
						final float minInlierRatio = 0.0f;
						final int minNumInliers = 20;
						final AffineModel2D model = new AffineModel2D();

						final Double z1 = pair._1()._1();
						final Double z2 = pair._2()._1();
						final ArrayList<Feature> features1 = pair._1()._2();
						final ArrayList<Feature> features2 = pair._2()._2();

						double inlierRatio = 0;

						if (features1.size() > 0 && features2.size() > 0) {
							final ArrayList<PointMatch> candidates = new ArrayList<PointMatch>();
							final ArrayList<PointMatch> inliers = new ArrayList<PointMatch>();

							FeatureTransform.matchFeatures(features1, features2, candidates, rod);

							boolean modelFound = false;
							try {
								modelFound = model.filterRansac(
										candidates,
										inliers,
										1000,
										maxEpsilon,
										minInlierRatio,
										minNumInliers,
										3);
							} catch (final NotEnoughDataPointsException e) {
								modelFound = false;
							}

							if (modelFound)
								inlierRatio = (double)inliers.size() / candidates.size();
						}
						return new Tuple2<Tuple2<Double, Double>, Double>(new Tuple2<Double, Double>(z1, z2), inlierRatio);
					}
				});

		/* inverse z-position lookup */
		final int n = zs.size();
		final HashMap<Double, Integer> zLUT = new HashMap<Double, Integer>();
		for (int i = 0; i < n; ++i)
			zLUT.put(zs.get(i), i);

		/* generate matrix */
		final FloatProcessor matrix = new FloatProcessor(n, n);
		matrix.add(Double.NaN);
		for (int i = 0; i < n; ++i)
			matrix.setf(i, i, 1.0f);

		matrix.setMinAndMax(0, 1);

		final float[] pixels = (float[])matrix.getPixels();

		/* aggregate */
		final float[] aggregate = similarity.aggregate(
				pixels,
				new Function2<float[], Tuple2<Tuple2<Double, Double>, Double>, float[]>() {

					@Override
					public float[] call(final float[] v1, final Tuple2<Tuple2<Double, Double>, Double> v2) throws Exception {
						/* generate matrix */
						final FloatProcessor matrix = new FloatProcessor(n, n, v1.clone());
						final int x = zLUT.get(v2._1()._1());
						final int y = zLUT.get(v2._1()._2());
						final float v = (float)v2._2().doubleValue();
						matrix.setf(x, y, v);
						matrix.setf(y, x, v);

						return (float[])matrix.getPixels();
					}
				},
				new Function2<float[], float[], float[]>() {

					@Override
					public float[] call(final float[] v1, final float[] v2) throws Exception {
						/* generate matrix */
						final float[] v3 = v1.clone();
						for (int i = 0; i < v3.length; ++i) {
							final float v = v2[i];
							if (!Float.isNaN(v))
								v3[i] = v;
						}

						return v3;
					}
				});

		IJ.saveAsTiff(new ImagePlus("matrix", new FloatProcessor(n, n, aggregate)), "/nobackup/saalfeld/tmp/matrix.tif");

		System.out.println("Done.");

        sc.close();

	}




	final static public void main2(final String... args) throws InterruptedException, ExecutionException, JsonIOException, JsonSyntaxException, IOException {

		final Options options = new Options(args);

		final String baseUrlString = options.getServer() +
                "/owner/" + options.getOwner() +
                "/project/" + options.getProjectId() +
                "/stack/" + options.getStackId();

        final URL zValuesUrl = new URL(baseUrlString + "/zValues");
        final ArrayList<Double> zs =
                JsonUtils.GSON.fromJson(
                        new InputStreamReader(zValuesUrl.openStream()),
                        new TypeToken<ArrayList<Double>>(){}.getType());


        final String urlString =
                baseUrlString +
                "/z/%f/box/" + (int)Math.round(options.getX()) + "," + (int)Math.round(options.getY()) + "," + (int)Math.round(options.getW()) + "," + (int)Math.round(options.getH()) + "," + options.getScale() +
                "/render-parameters";

		new ImageJ();

		final ImageStack stack = new ImageStack(
				(int)Math.ceil(options.getW() * options.getScale()),
				(int)Math.ceil(options.getH() * options.getScale()));

		ImagePlus imp = null;

		final int numThreads = 24;
		for (int i = 0; i < zs.size(); ++i) {
		    final int fi = i;
		    final ExecutorService exec = Executors.newFixedThreadPool(numThreads);
			final ArrayList<Future<BufferedImage>> futures = new ArrayList<Future<BufferedImage>>();
			for (int t = 0; t < numThreads && t + i <= zs.size(); ++t) {
				final int ft = t;
			    futures.add(exec.submit(new Callable<BufferedImage>() {

					@Override
					public BufferedImage call() throws IllegalArgumentException, IOException {
						final URL url = new URL(String.format(urlString, zs.get(fi + ft)));
						final RenderParameters param = RenderParameters.parseJson(new InputStreamReader(url.openStream()));

						final BufferedImage targetImage = param.openTargetImage();
						final ByteProcessor ip = new ByteProcessor(targetImage.getWidth(), targetImage.getHeight());
						mpicbg.ij.util.Util.fillWithNoise(ip);
						targetImage.getGraphics().drawImage(ip.createImage(), 0, 0, null);

						Render.render(param.getTileSpecs(), targetImage, param.getX(), param.getY(), param.getRes(param.getScale()), param.getScale(),
								param.isAreaOffset(), param.getNumberOfThreads(), param.skipInterpolation(), true);

						return targetImage;
					}
				}));
			}
			for (int f = 0; f < futures.size(); ++f) {
				stack.addSlice("" + zs.get(i + f), new ColorProcessor(futures.get(f).get()));
			}
			exec.shutdown();
			if (i == 0) {
				imp = new ImagePlus("export." + options.getProjectId() + "." + options.getStackId() + "." + options.getW() + "x" + options.getH() + "+" + options.getX() + "+" + options.getY(), stack);
				imp.show();
			} else {
				imp.setStack(stack);
				imp.updateAndDraw();
			}
		}
		System.out.println("Done.");
	}
}