/**
 *
 */
package org.janelia.saalfeldlab.sparkstuff;

import ij.ImageJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.janelia.alignment.Render;
import org.janelia.alignment.RenderParameters;
import org.janelia.alignment.Utils;
import org.janelia.workshop.spark.utility.Functions;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;

/**
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 *
 */
public class SimilarityRenderMontages {

	public static class Options {

		@Option(name = "-S", aliases = {"--server"}, required = true, usage = "Server base URL.")
		private String server = "http://tem-services.int.janelia.org:8080/render-ws/v1";

		@Option(name = "-u", aliases = {"--owner", "--user"}, required = true, usage = "Owner.")
		private String owner = "flyTEM";

		@Option(name = "-p", aliases = {"--project"}, required = true, usage = "Project ID.")
		private String projectId = "FAFB00";

		@Option(name = "-s", aliases = {"--stack"}, required = true, usage = "Stack ID.")
		private String stackId = "v5_align_tps";

		@Option(name = "-z", aliases = {"--minz"}, required = true, usage = "First z-index.")
		private Integer firstZ = 3451;

		@Option(name = "-Z", aliases = {"--maxz"}, required = true, usage = "Last z-index.")
		private Integer lastZ = 5799;

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

		public Integer getFirstZ() {
			return firstZ;
		}

		public Integer getLastZ() {
			return lastZ;
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

		final String urlString =
				options.getServer() +
				"/owner/" + options.getOwner() +
				"/project/" + options.getProjectId() +
				"/stack/" + options.getStackId() +
				"/z/%d/box/" + (int)Math.round(options.getX()) + "," + (int)Math.round(options.getY()) + "," + (int)Math.round(options.getW()) + "," + (int)Math.round(options.getH()) + "," + options.getScale() +
				"/render-parameters";

		final ArrayList<Integer> zs = new ArrayList<Integer>();

		for (int z = options.getFirstZ(); z <= options.getLastZ(); ++z) {
			zs.add(z);
		}

		final SparkConf conf      = new SparkConf().setAppName( "RenderSimilarities" );
        final JavaSparkContext sc = new JavaSparkContext(conf);

		final JavaRDD<Integer> rddZ = sc.parallelize(zs);
		final JavaPairRDD<Integer, String> urls = rddZ.mapToPair(
				new PairFunction<Integer, Integer, String>() {

					@Override
					public Tuple2<Integer, String> call(final Integer z) throws Exception {
						return new Tuple2<Integer, String>(z, String.format(urlString, z));
					}
				});

		final JavaPairRDD<Integer, RenderParameters> parameters = urls.mapToPair(
				new PairFunction<Tuple2<Integer, String>, Integer, RenderParameters>() {

					@Override
					public Tuple2<Integer, RenderParameters> call(final Tuple2<Integer, String> pair) throws Exception {
						return new Tuple2<Integer, RenderParameters>(
								pair._1(),
								RenderParameters.parseJson(new InputStreamReader(new URL(pair._2()).openStream())));
					}
				});

		final JavaPairRDD<Integer, BufferedImage> images = parameters.mapToPair(
				new PairFunction<Tuple2<Integer, RenderParameters>, Integer, BufferedImage>() {

					@Override
					public Tuple2<Integer, BufferedImage> call(final Tuple2<Integer, RenderParameters> pair) throws Exception {

						final RenderParameters param = pair._2();
						final BufferedImage targetImage = param.openTargetImage();
						final ByteProcessor ip = new ByteProcessor(targetImage.getWidth(), targetImage.getHeight());
						mpicbg.ij.util.Util.fillWithNoise(ip);
						targetImage.getGraphics().drawImage(ip.createImage(), 0, 0, null);

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

						return new Tuple2<Integer, BufferedImage>(
								pair._1(),
								targetImage);
					}
				});

		images.cache();

		/* TODO filter join without copy?  And then do pairwise comparisons. */

		/* ... */

		/* Instead, save some images for testing */
		final String outFormat = "/nobackup/saalfeld/tmp/spark-export/%d.png";
		final Integer goodFiles = images.aggregate(
				new Integer(0),
				new Function2<Integer, Tuple2<Integer, BufferedImage>, Integer>() {

					@Override
                    public final Integer call(
							final Integer count,
							final Tuple2<Integer, BufferedImage> pair) throws Exception {
						Utils.saveImage(
								pair._2(),
                                String.format(outFormat, pair._1()),
                                "png",
                                true,
                                0);
						return count + 1;
					}
				},
        		new Functions.MergeCount());

		System.out.println( String.format("Wrote %d files.", goodFiles));

        sc.close();

	}




	final static public void main2(final String... args) throws InterruptedException, ExecutionException {

		final Options options = new Options(args);

		final String urlString =
				options.getServer() +
				"/owner/" + options.getOwner() +
				"/project/" + options.getProjectId() +
				"/stack/" + options.getStackId() +
				"/z/%d/box/" + (int)Math.round(options.getX()) + "," + (int)Math.round(options.getY()) + "," + (int)Math.round(options.getW()) + "," + (int)Math.round(options.getH()) + "," + options.getScale() +
				"/render-parameters";

		new ImageJ();

		final ImageStack stack = new ImageStack(
				(int)Math.ceil(options.getW() * options.getScale()),
				(int)Math.ceil(options.getH() * options.getScale()));

		ImagePlus imp = null;

		final int numThreads = 24;
		for (int z = options.getFirstZ(); z <= options.getLastZ(); z += numThreads) {
			final ExecutorService exec = Executors.newFixedThreadPool(numThreads);
			final ArrayList<Future<BufferedImage>> futures = new ArrayList<Future<BufferedImage>>();
			for (int t = 0; t < numThreads && t + z <= options.getLastZ(); ++t) {
				final int fz = z;
				final int ft = t;
				futures.add(exec.submit(new Callable<BufferedImage>() {
//					@Override
					@Override
					public BufferedImage call() throws IllegalArgumentException, IOException {
						final URL url = new URL(String.format(urlString, fz + ft));
						final RenderParameters param = RenderParameters.parseJson(new InputStreamReader(url.openStream()));

						// final BufferedImage targetImage =
						// param.openTargetImage();
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
				stack.addSlice("" + (z + f), new ColorProcessor(futures.get(f).get()));
			}
			exec.shutdown();
			if (z == options.getFirstZ()) {
				imp = new ImagePlus("export." + options.getProjectId() + "." + options.getStackId() + ".z" + options.getFirstZ() + "-" + options.getLastZ() + "." + options.getW() + "x" + options.getH() + "+" + options.getX() + "+" + options.getY(), stack);
				imp.show();
			} else {
				imp.setStack(stack);
				imp.updateAndDraw();
			}
		}
		System.out.println("Done.");
	}
}
