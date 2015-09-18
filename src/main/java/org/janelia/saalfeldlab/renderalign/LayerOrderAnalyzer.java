package org.janelia.saalfeldlab.renderalign;

import ij.IJ;
import ij.ImagePlus;
import ij.process.FloatProcessor;

import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.models.Affine2D;
import mpicbg.models.IllDefinedDataPointsException;
import mpicbg.models.InterpolatedAffineModel2D;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;
import mpicbg.models.Tile;
import mpicbg.models.TileConfiguration;
import mpicbg.trakem2.transform.AffineModel2D;
import net.imglib2.img.ImagePlusAdapter;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.alignment.Render;
import org.janelia.alignment.RenderParameters;
import org.janelia.alignment.Utils;
import org.janelia.alignment.json.JsonUtils;
import org.janelia.alignment.spec.TileSpec;
import org.janelia.alignment.spec.TransformSpec;
import org.janelia.alignment.util.ImageProcessorCache;
import org.janelia.sort.tsp.TSP;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.python.google.common.io.Files;

import scala.Tuple2;

/**
 * Spark driver to determine layer ordering.
 *
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 */
public class LayerOrderAnalyzer {

    public static class Options
            implements Serializable {

        @Option(name = "-S", aliases = {"--server"}, required = true, usage = "Server base URL.")
        private String server = "http://tem-services.int.janelia.org:8080/render-ws/v1";

        @Option(name = "-u", aliases = {"--owner", "--user"}, required = true, usage = "Owner.")
        private String owner = "flyTEM";

        @Option(name = "-p", aliases = {"--project"}, required = true, usage = "Project ID.")
        private String projectId = "FAFB00";

        @Option(name = "-s", aliases = {"--stack"}, required = true, usage = "Stack ID.")
        private String stackId = "v5_align_tps";

        @Option(name = "-t", aliases = {"--scale"}, required = true, usage = "Scale.")
        private Double scale = 1.0 / 8.0;

        @Option(name = "-o", aliases = {"--outputPath"}, required = true, usage = "Output path.")
        private String outputPath = "/tmp/";

        @Option(name = "-r", aliases = {"--range"}, required = false,
                usage = "Range (maximum distance) for similarity comparisions.")
        private Integer range = 48;

        @Option(name = "-l", aliases = {"--maxLayersPerMatrix"}, required = false,
                usage = "Maximum number of layers to include in a similarity matrix.")
        private Integer maxLayersPerMatrix = 8000;

        @Option(name = "-f", aliases = {"--force"}, required = false,
                usage = "Regenerate montage image even if it exists.")
        private Boolean force = false;

        @Option(name = "-d", aliases = {"--fdSize"}, required = false,
                usage = "SIFT feature descriptor size (how many samples per row and column).")
        private Integer fdSize = 4;

        @Option(name = "-m", aliases = {"--minSIFTScale"}, required = false,
                usage = "SIFT minimum scale (minSize * minScale < size < maxSize * maxScale).")
        private Double minScale = 0.5;

        @Option(name = "-M", aliases = {"--maxSIFTScale"}, required = false,
                usage = "SIFT maximum scale (minSize * minScale < size < maxSize * maxScale).")
        private Double maxScale = 0.85;

        @Option(name = "-e", aliases = {"--steps"}, required = false,
                usage = "SIFT steps per scale octave.")
        private Integer steps = 3;

        @Option(name = "-c", aliases = {"--concordePath"}, required = true, usage = "Path to concorde executable.")
        private String concordePath = "concorde";

        public Options(final String[] args) {
            final CmdLineParser parser = new CmdLineParser(this);
            try {
                parser.parseArgument(args);

                final File layerImagesDir = getLayerImagesDir();
                if (!layerImagesDir.exists()) {
                    throw new IllegalArgumentException(layerImagesDir.getAbsolutePath() + " does not exist");
                }
            } catch (final Throwable t) {
                parser.printUsage(System.err);
                throw new RuntimeException(t);
            }

            if (!outputPath.endsWith("/")) {
                outputPath += "/";
            }
        }

        @Override
        public String toString() {
            return "Options{server='" + server + '\'' +
                   ", owner='" + owner + '\'' +
                   ", projectId='" + projectId + '\'' +
                   ", stackId='" + stackId + '\'' +
                   ", scale=" + scale +
                   ", outputPath='" + outputPath + '\'' +
                   ", range=" + range +
                   ", maxLayersPerMatrix=" + maxLayersPerMatrix +
                   ", force=" + force +
                   ", fdSize=" + fdSize +
                   ", minSIFTScale=" + minScale +
                   ", maxSIFTScale=" + maxScale +
                   ", steps=" + steps +
                   '}';
        }

        public String getBaseUrl() {
            return server + "/owner/" + owner + "/project/" + projectId + "/stack/" + stackId;
        }

        public File getLayerImagesDir() {
            return new File(outputPath, "layer_images");
        }

        public File getMontageFile(final Double z) {
            return new File(getLayerImagesDir(), z + ".png");
        }

    }

    public static void main(final String... args)
            throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {

        logInfo("*************** Job started! ***************");

        final Options options = new Options(args);

        logInfo("running " + LayerOrderAnalyzer.class + " with " + options);

        final String baseUrlString = options.getBaseUrl();

        final List<Double> zValues = getZValues(baseUrlString).subList(0, 2000);
//        final List<Double> zValues = getZValues(baseUrlString);

        final SparkConf conf = new SparkConf().setAppName("LayerOrderAnalyzer");

        // TODO: see if it's worth the trouble to use the faster KryoSerializer
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.registerKryoClasses(new Class[] { LayerFeatures.class, LayerSimilarity.class });

        final JavaSparkContext sc = new JavaSparkContext(conf);

        final String renderUrlFormat = baseUrlString + "/z/%f/render-parameters" +
                                       "?scale=" + options.scale + "&filter=true";

        final Map<Double, LayerFeatures> zToFeaturesMap = calculateFeatures(sc,
                                                                            options,
                                                                            zValues,
                                                                            renderUrlFormat);

        final Tuple2<List<Double>, Set<Double>> filteredZValues =
                filterOutLayersWithNoFeatures(
                        zValues,
                        zToFeaturesMap);

        final List<Double> zValuesWithFeatures = filteredZValues._1();
        final Set<Double> zValuesWithoutFeatures = filteredZValues._2();

        final List<Tuple2<Double, Double>> layerPairs = calculateLayerPairs(zValuesWithFeatures,
                                                                            options.range);
        final List<LayerSimilarity> similarities = calculateSimilarities(sc,
                                                                         zToFeaturesMap,
                                                                         layerPairs);

        exportMatchesForKhaled(similarities, zValues, options.outputPath, options.outputPath + "layer_images/");

        /* align the thing */

        /* make tiles */
        final HashMap<Double, Tile<?>> zTiles = new HashMap<>();
        for (final Double z : zValues)
            zTiles.put(
                    z,
                    new Tile(
                            new InterpolatedAffineModel2D<mpicbg.models.AffineModel2D, mpicbg.models.RigidModel2D>(
                                    new mpicbg.models.AffineModel2D(),
                                    new mpicbg.models.RigidModel2D(),
                                    1.0)));

        for (final LayerSimilarity sim : similarities) {
            if (sim.isModelFound()) {
                final double weight = sim.getInlierRatio();
                for (final PointMatch match : sim.getInliers()) {
                    match.setWeights(new double[]{weight});
                }

                final Tile t1 = zTiles.get(sim.getZ1());
                final Tile t2 = zTiles.get(sim.getZ2());
                t1.connect(t2, sim.getInliers());
            }
        }

        /* feed all tiles that have connections into tile configuration, report those that are disconnected */
        final TileConfiguration tc = new TileConfiguration();
        for (final Entry<Double, Tile<?>> entry : zTiles.entrySet()) {
            final Tile<?> t = entry.getValue();
            if (!t.getConnectedTiles().isEmpty())
                tc.addTile(entry.getValue());
            else
                logInfo(entry.getKey() + " is disconnected.");
        }

        /* three pass optimization, first using the regularizer exclusively ... */
        try {
            tc.preAlign();
            tc.optimize(0.01, 5000, 200, 0.9);
        } catch (NotEnoughDataPointsException | IllDefinedDataPointsException e) {
            e.printStackTrace();
        }
        /* ... then using the desired model with low regularization ... */
        for (final Tile<?> t : zTiles.values()) {
            ((InterpolatedAffineModel2D<?, ?>)t.getModel()).setLambda(0.1);
        }
        try {
            tc.optimize(0.01, 5000, 200, 0.9);
        } catch (NotEnoughDataPointsException | IllDefinedDataPointsException e) {
            e.printStackTrace();
        }
        /* ... then using the desired model with very low regularization.*/
        for (final Tile<?> t : zTiles.values()) {
            ((InterpolatedAffineModel2D<?, ?>)t.getModel()).setLambda(0.01);
        }
        try {
            tc.optimize(0.01, 5000, 200, 0.9);
        } catch (NotEnoughDataPointsException | IllDefinedDataPointsException e) {
            e.printStackTrace();
        }

        logInfo("Aligned.");

        final HashMap<Double, AffineTransform> zTransforms = new HashMap<>();

        final Rectangle2D.Double union = new Rectangle2D.Double();
        /* convert affine transforms from scaled to world space */
        /* I believe the global transform A is S^-1A'ST^-1, but I am tired */
        for (final Entry<Double, Tile<?>> entry : zTiles.entrySet()) {
            final Double z = entry.getKey();
            final Tile<?> t = entry.getValue();
            final LayerFeatures lf = zToFeaturesMap.get(z);
            final Rectangle2D.Double bounds = lf.getBounds();
            final AffineTransform affine = new AffineTransform();
            affine.scale(1.0 / options.scale, 1.0 / options.scale);
            affine.concatenate(((Affine2D)t.getModel()).createAffine());
            affine.scale(options.scale, options.scale);
            affine.translate(-bounds.x, -bounds.y);
            zTransforms.put(entry.getKey(), affine);
            union.add(affine.createTransformedShape(bounds).getBounds2D());
        }

        logInfo("Bounding box of aligned series is " + union.toString() + ".");

        logInfo("Affines:");
        for(final Entry<Double, AffineTransform> entry : zTransforms.entrySet()) {
            logInfo(entry.getKey() + " : " + entry.getValue());
        }

        final int transformedImages = renderTransformedMontages(
                sc,
                options,
                zValuesWithFeatures,
                zTransforms,
                union,
                renderUrlFormat,
                options.outputPath + "aligned/");

        logInfo("Rendered " + transformedImages + " images");


        sc.stop();

        buildSimilarityMatricesAndGenerateResults(zValues,
                                                  zValuesWithoutFeatures,
                                                  similarities,
                                                  options.outputPath,
                                                  options.maxLayersPerMatrix,
                                                  options.concordePath);

        logInfo("*************** Job done! ***************");
    }

    public static List<Double> getZValues(final String baseUrlString)
            throws IOException {

        final URL zValuesUrl = new URL(baseUrlString + "/zValues");

        final JsonUtils.Helper<Double> jsonHelper = new JsonUtils.Helper<>(Double.class);
        final List<Double> zValues = jsonHelper.fromJsonArray(new InputStreamReader(zValuesUrl.openStream()));

        logInfo("retrieved " + zValues.size() + " values from " + zValuesUrl);

        return zValues;
    }

    private static Map<Double, LayerFeatures> calculateFeatures(final JavaSparkContext sc,
                                                                final Options options,
                                                                final List<Double> zValues,
                                                                final String renderUrlFormat) {

        final FloatArray2DSIFT.Param siftParameters = new FloatArray2DSIFT.Param();
        siftParameters.fdSize = options.fdSize;
        siftParameters.steps = options.steps;

        final JavaRDD<Double> rddZ = sc.parallelize(zValues);

        final JavaRDD<LayerFeatures> rddFeatures = rddZ.map(new Function<Double, LayerFeatures>() {
            @Override
            public LayerFeatures call(final Double z)
                    throws Exception {
                setupExecutorLog4j("z:" + z);
                final LayerFeatures layerFeatures = new LayerFeatures(z);
                final String renderParametersUrlString = String.format(renderUrlFormat, z);
                layerFeatures.extractFeatures(
                        layerFeatures.loadMontage(renderParametersUrlString, options.getMontageFile(z), options.force),
                        siftParameters,
                        options.minScale,
                        options.maxScale);
                return layerFeatures;
            }
        });

        final List<LayerFeatures> driverFeatures = rddFeatures.collect();
        logInfo("collected feature lists for " + driverFeatures.size() + " layers");

        long totalFeatureCount = 0;

        final Map<Double, LayerFeatures> driverZtoFeaturesMap = new HashMap<>(driverFeatures.size() * 2);
        for (final LayerFeatures layerFeatures : driverFeatures) {
            if (layerFeatures.size() > 20) {
                driverZtoFeaturesMap.put(layerFeatures.getZ(), layerFeatures);
                totalFeatureCount += layerFeatures.size();
            } else {
                System.out.println("WARNING: excluding layer " + layerFeatures.getZ() +
                                   " because " + layerFeatures.size() + " features were found for it, " +
                                   layerFeatures.getProcessingMessages());
            }
        }

        logInfo("total feature count is " + totalFeatureCount);

        return driverZtoFeaturesMap;
    }

    private static Tuple2<List<Double>, Set<Double>> filterOutLayersWithNoFeatures(
            final List<Double> zValues,
            final Map<Double, LayerFeatures> zToFeaturesMap) {
        final List<Double> zValuesWithFeatures;
        final Set<Double> zValuesWithoutFeatures;

        if (zValues.size() == zToFeaturesMap.size()) {
            zValuesWithFeatures = zValues;
            zValuesWithoutFeatures = new HashSet<>();
        } else {
            zValuesWithFeatures = new ArrayList<>(zValues.size());
            zValuesWithoutFeatures = new HashSet<>();
            for (final Double z : zValues) {
                if (zToFeaturesMap.containsKey(z)) {
                    zValuesWithFeatures.add(z);
                } else {
                    zValuesWithoutFeatures.add(z);
                }
            }
        }

        return new Tuple2<>(zValuesWithFeatures, zValuesWithoutFeatures);
    }

    private static List<Tuple2<Double, Double>> calculateLayerPairs(final List<Double> zValues,
                                                                    final int range) {
        final int n = zValues.size();
        final List<Tuple2<Double, Double>> layerPairs = new ArrayList<>(n * range);
        for (int i = 0; i < n; i++) {
            for (int k = i + 1; k < n && k < i + range; k++) {
                layerPairs.add(new Tuple2<>(zValues.get(i), zValues.get(k)));
            }
        }

        logInfo("derived " + layerPairs.size() + " layer pairs");

        return layerPairs;
    }

    private static List<LayerSimilarity> calculateSimilarities(final JavaSparkContext sc,
                                                               final Map<Double, LayerFeatures> zToFeaturesMap,
                                                               final List<Tuple2<Double, Double>> zPairs) {

        // broadcast feature map to all nodes for use in similarity calculation
        final Broadcast<Map<Double, LayerFeatures>> broadcastZToFeaturesMap = sc.broadcast(zToFeaturesMap);

        final JavaRDD<Tuple2<Double, Double>> rddZPairs = sc.parallelize(zPairs);

        final JavaRDD<LayerSimilarity> rddSimilarity = rddZPairs.map(
                new Function<Tuple2<Double, Double>, LayerSimilarity>() {

                    @Override
                    public LayerSimilarity call(final Tuple2<Double, Double> tuple2)
                            throws Exception {
                        final Double z1 = tuple2._1();
                        final Double z2 = tuple2._2();
                        setupExecutorLog4j("z1:" + z1 + ",z2:" + z2);
                        final LayerSimilarity layerSimilarity = new LayerSimilarity(z1, z2);
                        layerSimilarity.calculateInlierRatio(broadcastZToFeaturesMap.getValue());
                        return layerSimilarity;
                    }
                });

        final List<LayerSimilarity> driverSimilarities = rddSimilarity.collect();
        logInfo("collected similarities for " + driverSimilarities.size() + " layer pairs");

        return driverSimilarities;
    }

    private static void buildSimilarityMatricesAndGenerateResults(final List<Double> zValues,
                                                                  final Set<Double> zValuesWithoutFeatures,
                                                                  final List<LayerSimilarity> similarities,
                                                                  final String outputPath,
                                                                  final int maxLayersPerMatrix,
                                                                  final String concordePath)
            throws IOException, InterruptedException {

        Collections.sort(zValues);
        Collections.sort(similarities);

        if (maxLayersPerMatrix < zValues.size()) {

            final List<LayerSimilarity> filteredSimilarityList = new ArrayList<>(similarities.size());

            final double zValueDelta = 0.0001;
            final int overlap = maxLayersPerMatrix / 2;

            int maxZIndex;
            double minimumZ;
            double maximumZ;
            double z1;
            for (int zIndex = 0; zIndex < zValues.size(); zIndex = zIndex + overlap) {

                maxZIndex = zIndex + maxLayersPerMatrix;
                if (maxZIndex > zValues.size()) {
                    maxZIndex = zValues.size();
                }

                minimumZ = zValues.get(zIndex) - zValueDelta;
                maximumZ = zValues.get(maxZIndex - 1) + zValueDelta;

                for (final LayerSimilarity similarity : similarities) {
                    z1 = similarity.getZ1();
                    if (z1 > minimumZ) {
                        if (z1 < maximumZ) {
                            if (similarity.getZ2() < maximumZ) {
                                filteredSimilarityList.add(similarity);
                            } // else skip
                        } else {
                            break;
                        }
                    } // else skip
                }

                buildSimilarityMatrixAndGenerateResults(zValues.subList(zIndex, maxZIndex),
                                                        zValuesWithoutFeatures,
                                                        filteredSimilarityList,
                                                        outputPath,
                                                        concordePath);

                if ((zIndex + overlap) < zValues.size()) {
                    filteredSimilarityList.clear();
                } else {
                    break;
                }

            }

        } else {

            buildSimilarityMatrixAndGenerateResults(zValues,
                                                    zValuesWithoutFeatures,
                                                    similarities,
                                                    outputPath,
                                                    concordePath);

        }


    }

    private static void buildSimilarityMatrixAndGenerateResults(final List<Double> zValues,
                                                                final Set<Double> zValuesWithoutFeatures,
                                                                final List<LayerSimilarity> similarityList,
                                                                final String outputPath,
                                                                final String concordePath) {

        try {
            final double firstZ = zValues.get(0);
            final double lastZ = zValues.get(zValues.size() - 1);
            final String similarityDirName = String.format("similarities_%05.2f_%05.2f", firstZ, lastZ);
            final File similarityDir = new File(outputPath, similarityDirName).getCanonicalFile();
            final File matrixFile = new File(similarityDir, "matrix.tif");

            if (similarityDir.exists()) {
                Files.deleteRecursively(similarityDir);
            }

            if (! similarityDir.mkdir()) {
                throw new IllegalArgumentException("failed to create " + similarityDir.getAbsolutePath());
            }

            final ImagePlus matrixImagePlus = buildSimilarityMatrix(zValues,
                                                                    zValuesWithoutFeatures,
                                                                    similarityList,
                                                                    matrixFile);

            logInfo("generating TSP results for layers " + firstZ + " to " + lastZ);


            TSP.generateResultFiles(ImagePlusAdapter.wrapFloat(matrixImagePlus),
                                    concordePath,
                                    similarityDir.getAbsolutePath());
        } catch (final Throwable t) {

            logInfo("buildSimilarityMatrixAndGenerateResults: caught exception");
            t.printStackTrace();

        }
    }

    private static ImagePlus buildSimilarityMatrix(final List<Double> zValues,
                                                   final Set<Double> zValuesWithoutFeatures,
                                                   final List<LayerSimilarity> similarities,
                                                   final File matrixFile) {

        final int n = zValues.size();

        logInfo("generating " + n + "x" + n + " similarity matrix from " + similarities.size() + " results");

        // inverse z-position lookup
        final HashMap<Double, Integer> zLUT = new HashMap<>();
        for (int i = 0; i < n; ++i) {
            zLUT.put(zValues.get(i), i);
        }

        final FloatProcessor matrix = new FloatProcessor(n, n);
        matrix.add(Double.NaN);
        for (int i = 0; i < n; ++i) {
            if (! zValuesWithoutFeatures.contains(zValues.get(i))) {
                matrix.setf(i, i, 1.0f);
            }
        }

        matrix.setMinAndMax(0, 1);

        for (final LayerSimilarity similarity : similarities) {
            final int x = zLUT.get(similarity.getZ1());
            final int y = zLUT.get(similarity.getZ2());
            final float v = similarity.getInlierRatio().floatValue();

            matrix.setf(x, y, v);
            //noinspection SuspiciousNameCombination
            matrix.setf(y, x, v);
        }

        logInfo("saving " + matrixFile.getAbsolutePath());

        final ImagePlus matrixImagePlus = new ImagePlus("matrix", matrix);
        IJ.saveAsTiff(matrixImagePlus, matrixFile.getAbsolutePath());

        return matrixImagePlus;
    }

    private static int renderTransformedMontages(
            final JavaSparkContext sc,
            final Options options,
            final List<Double> zValues,
            final Map<Double, AffineTransform> transforms,
            final Rectangle2D.Double bounds,
            final String renderUrlFormat,
            final String outputPath) {

        final JavaRDD<Double> rddZ = sc.parallelize(zValues);
        final JavaRDD<Boolean> rddFeatures = rddZ.map(new Function<Double, Boolean>() {
            @Override
            public Boolean call(final Double z) throws Exception {
                setupExecutorLog4j("z:" + z);

                final String renderParametersUrlString = String.format(renderUrlFormat, z);
                final RenderParameters renderParameters = RenderParameters.loadFromUrl(renderParametersUrlString);

                logInfo("loadMontage: retrieved " + renderParametersUrlString);

                /* attach global affine to TileSpecs */
                final AffineModel2D affine = new AffineModel2D();
                affine.set(transforms.get(z));
                for (final TileSpec spec : renderParameters.getTileSpecs()) {
                    final ArrayList<TransformSpec> l = new ArrayList<>();
                    l.add(TransformSpec.create(affine));
                    spec.addTransformSpecs(l);
                }

                /* set bounding box */
                renderParameters.x = bounds.x;
                renderParameters.y = bounds.y;
                renderParameters.width = (int) Math.ceil(bounds.width);
                renderParameters.height = (int) Math.ceil(bounds.height);

                final BufferedImage montageImage = renderParameters.openTargetImage();
                Render.render(renderParameters, montageImage, ImageProcessorCache.DISABLED_CACHE);

                boolean success = false;
                try {
                    Utils.saveImage(montageImage, outputPath + z + ".png", "png", true, 9);
                    success = true;
                } catch (final Throwable t) {
                    logInfo("renderTransformedMontage: failed to save " + outputPath + z);
                    t.printStackTrace();
                }

                return new Boolean(success);
            }
        });
        return rddFeatures.aggregate(new Integer(0), new Function2<Integer, Boolean, Integer>() {
            @Override
            public Integer call(Integer arg0, final Boolean arg1) throws Exception {
                if (arg1)
                    return ++arg0;
                else
                    return arg0;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer arg0, final Integer arg1) throws Exception {
                return arg0 + arg1;
            }
        });
    }


    private static void exportMatchesForKhaled(
            final Iterable<LayerSimilarity> similarities,
            final Iterable<Double> zValues,
            final String khaledExportPath,
            final String montageExportPath) {
        try (final FileOutputStream fos = new FileOutputStream(khaledExportPath + "khaled-matches.txt");
                final OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
                final FileOutputStream fos2 = new FileOutputStream(khaledExportPath + "khaled-ids.txt");
                final OutputStreamWriter out2 = new OutputStreamWriter(fos2, "UTF-8");) {
            for (final LayerSimilarity ls : similarities) {
                final long id1 = Double.doubleToLongBits(ls.getZ1());
                final long id2 = Double.doubleToLongBits(ls.getZ2());
                if (ls.isModelFound() == true) {
                    for (final PointMatch p : ls.getInliers()) {
                        final double[] p1 = p.getP1().getL();
                        final double[] p2 = p.getP2().getL();
                        out.write(id1 + "\t" + p1[0] + "\t" + p1[1] + "\t" + id2 + "\t" + p2[0] + "\t" + p2[1] + "\n");
                    }
                }
            }
            out.close();
            for (final Double z : zValues) {
                final long id = Double.doubleToLongBits(z);
                out2.write(id + "\t" + montageExportPath + z + ".png\n");
            }
            out2.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }


    public static void setupExecutorLog4j(final String context) {
        final Logger logger = LogManager.getLogger("org.janelia");

        for (final Enumeration e = LogManager.getRootLogger().getAllAppenders(); e.hasMoreElements(); ) {
            final Appender a = (Appender) e.nextElement();
            if (a instanceof ConsoleAppender) {
                final Layout layout = a.getLayout();
                if (layout instanceof PatternLayout) {
                    final PatternLayout patternLayout = (PatternLayout) layout;
                    final String conversionPattern = "%d{ISO8601} [%t] [%X{context}] %-5p [%c] %m%n";
                    if (!conversionPattern.equals(patternLayout.getConversionPattern())) {
                        a.setLayout(new PatternLayout(conversionPattern));
                    }
                }
            }
        }

        MDC.put("context", context);

        logger.setLevel(Level.DEBUG);
    }

    private static void logInfo(final String message) {
        System.out.println("\n" + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())) + " " + message);
    }
}
