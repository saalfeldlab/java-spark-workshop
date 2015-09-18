package org.janelia.saalfeldlab.renderalign;

import com.google.gson.Gson;

import ij.ImagePlus;
import ij.process.ByteProcessor;

import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.util.Timer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.janelia.alignment.Render;
import org.janelia.alignment.RenderParameters;
import org.janelia.alignment.Utils;
import org.janelia.alignment.spec.Bounds;
import org.janelia.alignment.util.ImageProcessorCache;

/**
 * Collection of features extracted from a layer montage.
 *
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 */
public class LayerFeatures implements Serializable {

    private Double z;
    private String renderParametersUrlString;
    private String boundsUrlString;
    private File montageFile;
    private File featureListFile;

    // derived data
    private Rectangle2D.Double bounds;
    private Integer featureCount;
    private String processingMessages;

    public LayerFeatures(final Double z,
                         final String renderParametersUrlString,
                         final String boundsUrlString,
                         final File montageFile,
                         final File featureListFile) {
        this.z = z;
        this.renderParametersUrlString = renderParametersUrlString;
        this.boundsUrlString = boundsUrlString;
        this.montageFile = montageFile;
        this.featureListFile = featureListFile;
    }

    public Double getZ() {
        return z;
    }

    public File getFeatureListFile() {
        return featureListFile;
    }

    public Rectangle2D.Double getBounds() {
        return bounds;
    }

    public Integer getFeatureCount() {
        return featureCount;
    }

    public String getProcessingMessages() {
        return processingMessages;
    }

    @Override
    public String toString() {
        return "LayerFeatures{z=" + z +
               ", renderParametersUrlString='" + renderParametersUrlString + '\'' +
               ", boundsUrlString='" + boundsUrlString + '\'' +
               ", montageFile=" + montageFile +
               ", featureListFile=" + featureListFile +
               ", bounds=" + bounds +
               ", featureCount=" + featureCount +
               ", processingMessages='" + processingMessages + '\'' +
               '}';
    }

    public List<Feature> loadMontageAndExtractFeatures(final boolean forceMontageRendering,
                                                       final FloatArray2DSIFT.Param siftParameters,
                                                       final double minScale,
                                                       final double maxScale,
                                                       final boolean forceFeatureExtraction)
            throws IllegalStateException, IOException {

        final List<Feature> featureList;

        if (forceMontageRendering || forceFeatureExtraction ||
            (featureListFile == null) || (! featureListFile.exists())) {

            final BufferedImage montageImage = loadMontage(forceMontageRendering);
            featureList = extractFeatures(montageImage, siftParameters, minScale, maxScale);

            if (featureListFile != null) {
                writeFeatureListToFile(featureList, featureListFile);
            }

        } else {
            featureList = readFeatureListFromFile(featureListFile);
            featureCount = featureList.size();
            bounds = loadBounds(boundsUrlString);
        }

        return featureList;
    }

    public List<Feature> getPersistedFeatureList()
            throws IllegalArgumentException {
        return readFeatureListFromFile(featureListFile);
    }

    /**
     * Loads the layer's montage image either from disk or by rendering it.
     *
     * @param  force if true, montage will be re-rendered even if a
     *               cached version already exists on disk.
     */
    public BufferedImage loadMontage(final boolean force) {

        LOG.info("loadMontage: entry, z=" + z);

        final Timer timer = new Timer();
        timer.start();

        final BufferedImage montageImage;
        if (force || (montageFile == null) || (! montageFile.exists())) {

            final RenderParameters renderParameters = RenderParameters.loadFromUrl(renderParametersUrlString);

            LOG.info("loadMontage: retrieved " + renderParametersUrlString);

            bounds = new Rectangle2D.Double(
            		renderParameters.getX(),
            		renderParameters.getY(),
            		renderParameters.getWidth(),
            		renderParameters.getHeight());

            montageImage = renderParameters.openTargetImage();
            final ByteProcessor ip = new ByteProcessor(montageImage.getWidth(), montageImage.getHeight());

            mpicbg.ij.util.Util.fillWithNoise(ip);
            montageImage.getGraphics().drawImage(ip.createImage(), 0, 0, null);

            Render.render(renderParameters, montageImage, ImageProcessorCache.DISABLED_CACHE);

            if (montageFile != null) {
                try {
                    Utils.saveImage(
                            montageImage,
                            montageFile.getAbsolutePath(),
                            "png",
                            true,
                            9);
                } catch (final Throwable t) {
                    LOG.warn("loadMontage: failed to save " + montageFile.getAbsolutePath(), t);
                }
            }

        } else {

            final ImagePlus ip = Utils.openImagePlus(montageFile.getAbsolutePath());
            montageImage = ip.getBufferedImage();
            LOG.info("loadMontage: loaded " + montageFile.getAbsolutePath());

            if (boundsUrlString != null) {
                bounds = loadBounds(boundsUrlString);
            }
        }

        LOG.info("loadMontage: exit, z=" + z + ", elapsedTime=" + (timer.stop() / 1000) + "s");

        return montageImage;
    }

    /**
     * Use {@link SIFT} to extract features from montage image.
     *
     * @throws IllegalStateException
     *   if {@link #loadMontage} was not called first to load the montage image into memory.
     */
    public List<Feature> extractFeatures(final BufferedImage montageImage,
                                         final FloatArray2DSIFT.Param siftParameters,
                                         final double minScale,
                                         final double maxScale) throws IllegalStateException {

        LOG.info("extractFeatures: entry, z=" + z);

        final Timer timer = new Timer();
        timer.start();


        final FloatArray2DSIFT.Param localSiftParameters = siftParameters.clone();
        final int w = montageImage.getWidth();
        final int h = montageImage.getHeight();
        final int minSize = w < h ? w : h;
        final int maxSize = w > h ? w : h;
        localSiftParameters.minOctaveSize = (int)(minScale * minSize - 1.0);
        localSiftParameters.maxOctaveSize = (int)Math.round(maxScale * maxSize);

        // Let imagePlus determine correct processor - original use of ColorProcessor resulted in
        // fewer extracted features when montageImage was loaded from disk.
        final ImagePlus imagePlus = new ImagePlus("", montageImage);

        final FloatArray2DSIFT sift = new FloatArray2DSIFT(localSiftParameters);
        final SIFT ijSIFT = new SIFT(sift);

        final List<Feature> featureList = new ArrayList<>();
        ijSIFT.extractFeatures(imagePlus.getProcessor(), featureList);

        this.featureCount = featureList.size();

        if (featureList.size() == 0) {

            final StringBuilder sb = new StringBuilder(256);
            sb.append("no features were extracted");

            if (montageImage.getWidth() < siftParameters.minOctaveSize) {
                sb.append(" because montage image width (").append(montageImage.getWidth());
                sb.append(") is less than SIFT minOctaveSize (").append(siftParameters.minOctaveSize).append(")");
            } else if (montageImage.getHeight() < siftParameters.minOctaveSize) {
                sb.append(" because montage image height (").append(montageImage.getHeight());
                sb.append(") is less than SIFT minOctaveSize (").append(siftParameters.minOctaveSize).append(")");
            } else if (montageImage.getWidth() > siftParameters.maxOctaveSize) {
                sb.append(" because montage image width (").append(montageImage.getWidth());
                sb.append(") is greater than SIFT maxOctaveSize (").append(siftParameters.maxOctaveSize).append(")");
            } else if (montageImage.getHeight() > siftParameters.maxOctaveSize) {
                sb.append(" because montage image height (").append(montageImage.getHeight());
                sb.append(") is greater than SIFT maxOctaveSize (").append(siftParameters.maxOctaveSize).append(")");
            } else {
                sb.append(", not sure why, montage image width (").append(montageImage.getWidth());
                sb.append(") or height (").append(montageImage.getHeight());
                sb.append(") may be less than maxKernelSize derived from SIFT steps(");
                sb.append(siftParameters.steps).append(")");
            }

            this.processingMessages = sb.toString();
        }

        LOG.info("extractFeatures: exit, extracted " + featureList.size() + " features for z=" + z +
                 ", elapsedTime=" + (timer.stop() / 1000) + "s");

        return featureList;
    }

    public static void writeFeatureListToFile(final List<Feature> featureList,
                                              final File file)
            throws IOException {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file))) {
            objectOutputStream.writeObject(featureList);
        }
        LOG.info("writeFeatureListToFile: wrote " + featureList.size() + " features to " + file.getAbsolutePath());
    }

    public static List<Feature> readFeatureListFromFile(final File file)
            throws IllegalArgumentException {

        if (file == null) {
            throw new IllegalArgumentException("feature list file not specified");
        }

        if (! file.exists()) {
            throw new IllegalArgumentException("feature list file " + file.getAbsolutePath() + " does not exist");
        }

        final List<Feature> featureList;
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file))) {
            //noinspection unchecked
            featureList = (List<Feature>) objectInputStream.readObject();
        } catch (Throwable t) {
            throw new IllegalArgumentException("failed to load feature list from " + file.getAbsolutePath(), t);
        }

        LOG.info("readFeatureListFromFile: read " + featureList.size() + " features from " + file.getAbsolutePath());

        return featureList;
    }

    public static void main(final String[] args) {

        if (args.length < 3) {
            throw new IllegalArgumentException("USAGE: java " + LayerFeatures.class +
                                               " <z> <renderParametersUrl> <boundsUrl>" +
                                               " [montageFile] [featureListFile]" +
                                               " [forceMontageRendering] [forceFeatureExtraction]");
        }

        final Double z = Double.parseDouble(args[0]);
        final String renderParametersUrlString = args[1];
        final String boundsUrlString = args[2];

        File montageFile = null;
        File featureListFile = null;
        boolean forceMontageRendering = false;
        boolean forceFeatureExtraction = false;

        if (args.length > 3) {
            montageFile = new File(args[3]);
            if (args.length > 4) {
                featureListFile = new File(args[4]);
                if (args.length > 5) {
                    forceMontageRendering = Boolean.parseBoolean(args[5]);
                    if (args.length > 6) {
                        forceFeatureExtraction = Boolean.parseBoolean(args[6]);
                    }
                }
            }
        }

        final LayerFeatures layerFeatures = new LayerFeatures(z,
                                                              renderParametersUrlString,
                                                              boundsUrlString,
                                                              montageFile,
                                                              featureListFile);
        final List<Feature> featureList;
        try {
            featureList = layerFeatures.loadMontageAndExtractFeatures(forceMontageRendering,
                                                                      new FloatArray2DSIFT.Param(),
                                                                      0.5,
                                                                      0.85,
                                                                      forceFeatureExtraction);
            System.out.println("extracted " + featureList.size() + " features");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static Rectangle2D.Double loadBounds(final String url) {

        Bounds bounds;
        InputStream urlStream = null;
        try {
            try {
                final URL urlObject = new URL(url);
                urlStream = urlObject.openStream();
            } catch (final Throwable t) {
                throw new IllegalArgumentException("failed to load bounds from " + url, t);
            }

            bounds = new Gson().fromJson(new InputStreamReader(urlStream), Bounds.class);

        } finally {
            if (urlStream != null) {
                try {
                    urlStream.close();
                } catch (final IOException e) {
                    LOG.warn("failed to close " + url + ", ignoring error", e);
                }
            }
        }

        final Rectangle2D.Double boundingBox = new Rectangle2D.Double(
                bounds.getMinX(),
                bounds.getMinY(),
                bounds.getMaxX() - bounds.getMinX() + 1,
                bounds.getMaxY() - bounds.getMinY() + 1);

        LOG.info("loadBounds: loaded " + boundingBox + " from " + url);

        return boundingBox;
    }


    private static final Logger LOG = LogManager.getLogger(LayerFeatures.class);
}