package org.janelia.saalfeldlab.renderalign;

import ij.ImagePlus;
import ij.process.ByteProcessor;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.util.Timer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.janelia.alignment.Render;
import org.janelia.alignment.RenderParameters;
import org.janelia.alignment.Utils;
import org.janelia.alignment.util.ImageProcessorCache;

/**
 * Collection of features extracted from a layer montage.
 *
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 */
public class LayerFeatures implements Serializable {

    public static final FloatArray2DSIFT.Param DEFAULT_SIFT_PARAMETERS = new FloatArray2DSIFT.Param();
    static {
        DEFAULT_SIFT_PARAMETERS.fdSize = 4;
        DEFAULT_SIFT_PARAMETERS.maxOctaveSize = 3000;
        DEFAULT_SIFT_PARAMETERS.minOctaveSize = 800;
        DEFAULT_SIFT_PARAMETERS.steps = 3;
    }

    private Double z;
    private List<Feature> featureList;
    private String processingMessages;

    private transient BufferedImage montageImage;

    public LayerFeatures(final Double z) {
        this.z = z;
        this.featureList = new ArrayList<>();
        this.montageImage = null;
        this.processingMessages = null;
    }

    public Double getZ() {
        return z;
    }

    public List<Feature> getFeatureList() {
        return featureList;
    }

    public String getProcessingMessages() {
        return processingMessages;
    }

    @Override
    public String toString() {
        return "{\"z\": " + z +
               ", \"featureListSize\": " + size() +
               '}';
    }

    /**
     * @return number of extracted features.
     */
    public int size() {
        return featureList.size();
    }

    /**
     * Adds this layer's feature list to the specified map (with z value keys).
     */
    public List<Feature> addToMap(final Map<Double, List<Feature>> map) {
        return map.put(z, featureList);
    }

    /**
     * Loads the layer's montage image either from disk or by rendering it.
     *
     * @param  renderParametersUrlString  URL for render parameters in case montage needs to be rendered.
     *
     * @param  montageFile                (optional) cached montage on disk.
     *
     * @param  force                      if true, montage will be re-rendered even if a
     *                                    cached version already exists on disk.
     */
    public void loadMontage(final String renderParametersUrlString,
                            final File montageFile,
                            final boolean force) {

        LOG.info("loadMontage: entry, z=" + z);

        final Timer timer = new Timer();
        timer.start();

        if (force || (montageFile == null) || (! montageFile.exists())) {

            final RenderParameters renderParameters = RenderParameters.loadFromUrl(renderParametersUrlString);

            LOG.info("loadMontage: retrieved " + renderParametersUrlString);

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

        }

        LOG.info("loadMontage: exit, z=" + z + ", elapsedTime=" + (timer.stop() / 1000) + "s");
    }

    /**
     * Use {@link SIFT} to extract features from montage image.
     *
     * @throws IllegalStateException
     *   if {@link #loadMontage} was not called first to load the montage image into memory.
     */
    public void extractFeatures(final FloatArray2DSIFT.Param siftParameters) throws IllegalStateException {

        LOG.info("extractFeatures: entry, z=" + z);

        if (montageImage == null) {
            throw new IllegalStateException("loadMontage must be called before extracting features");
        }

        final Timer timer = new Timer();
        timer.start();

        // Let imagePlus determine correct processor - original use of ColorProcessor resulted in
        // fewer extracted features when montageImage was loaded from disk.
        final ImagePlus imagePlus = new ImagePlus("", montageImage);

        final FloatArray2DSIFT sift = new FloatArray2DSIFT(siftParameters);
        final SIFT ijSIFT = new SIFT(sift);

        ijSIFT.extractFeatures(imagePlus.getProcessor(), featureList);

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
    }

    public static void main(final String[] args) {

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: java " + LayerFeatures.class +
                                               " <z> <renderUrl> <montageFile> <force>");
        }

        final Double z = Double.parseDouble(args[0]);
        final String renderParametersUrlString = args[1];
        final File montageFile = new File(args[2]);
        final boolean force = Boolean.parseBoolean(args[3]);

        final LayerFeatures layerFeatures = new LayerFeatures(z);
        layerFeatures.loadMontage(renderParametersUrlString,
                                  montageFile,
                                  force);
        layerFeatures.extractFeatures(DEFAULT_SIFT_PARAMETERS);

        final List<Feature> featureList = layerFeatures.getFeatureList();
        System.out.println("extracted " + featureList.size() + " features");
    }

    private static final Logger LOG = LogManager.getLogger(LayerFeatures.class);

    public int getWidth() {
    	return montageImage.getWidth();
    }

    public int getHeight() {
    	return montageImage.getHeight();
    }
}