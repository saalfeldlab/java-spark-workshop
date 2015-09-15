package org.janelia.saalfeldlab.renderalign;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import mpicbg.ij.FeatureTransform;
import mpicbg.imagefeatures.Feature;
import mpicbg.models.NotEnoughDataPointsException;
import mpicbg.models.PointMatch;
import mpicbg.trakem2.transform.AffineModel2D;
import mpicbg.util.Timer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Calculates the similarity between two layers based on supplied features.
 *
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 */
public class LayerSimilarity implements Serializable, Comparable<LayerSimilarity> {

    private Double z1;
    private Double z2;
    private Double inlierRatio;
    private AffineModel2D model;
    private boolean modelFound;

    public LayerSimilarity(final Double z1,
                           final Double z2) {
        this.z1 = z1;
        this.z2 = z2;
        this.inlierRatio = null;
    }

    public Double getZ1() {
        return z1;
    }

    public Double getZ2() {
        return z2;
    }

    public Double getInlierRatio() {
        return inlierRatio;
    }

    public AffineModel2D getModel() {
        return model;
    }

    public boolean isModelFound() {
        return modelFound;
    }

    @Override
    public String toString() {
        final String dataString = (model == null) ? "" : getModel().toDataString();
        return "{\"z1\": " + getZ1() +
               ", \"z2\": " + getZ2() +
               ", \"inlierRatio\": " + getInlierRatio() +
               ", \"modelFound\": " + isModelFound() +
               ", \"dataString\": \"" + dataString + "\"}";
    }

    @Override
    public int compareTo(@Nonnull final LayerSimilarity that) {
        int result = this.z1.compareTo(that.z1);
        if (result == 0) {
            result = this.z2.compareTo(that.z2);
        }
        return result;
    }

    /**
     * Calculates the inlier ratio ({@link #getInlierRatio()}) between the two layers.
     *
     * @param  zToFeaturesMap  map of z values to feature lists for each layer.
     *
     * @throws IllegalArgumentException
     *   if features cannot be found for both layers.
     */
    public void calculateInlierRatio(final Map<Double, List<Feature>> zToFeaturesMap)
            throws IllegalArgumentException {

        LOG.info("calculateInlierRatio: entry");

        final float rod = 0.92f;
        final float maxEpsilon = 50f;
        final float minInlierRatio = 0.0f;
        final int minNumInliers = 20;

        model = new AffineModel2D();

        final List<Feature> features1 = getFeatureList(z1, zToFeaturesMap);
        final List<Feature> features2 = getFeatureList(z2, zToFeaturesMap);

        inlierRatio = 0.0;

        final Timer timer = new Timer();
        timer.start();

        final ArrayList<PointMatch> candidates = new ArrayList<>();
        final ArrayList<PointMatch> inliers = new ArrayList<>();

        FeatureTransform.matchFeatures(features1, features2, candidates, rod);

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

        if (modelFound) {
            inlierRatio = (double) inliers.size() / candidates.size();
        }

        LOG.info("calculateInlierRatio: exit, layerSimilarity=" + this +
                 ", elapsedTime=" + (timer.stop() / 1000) + "s");
    }

    private List<Feature> getFeatureList(Double z,
                                         Map<Double, List<Feature>> zToFeaturesMap)
            throws IllegalArgumentException {

        final List<Feature> featureList = zToFeaturesMap.get(z);

        if (featureList == null) {
            throw new IllegalArgumentException("feature list for " + z + " is missing");
        }

        if (featureList.size() == 0) {
            throw new IllegalArgumentException("feature list for " + z + " is empty");
        }

        return featureList;
    }

    private static final Logger LOG = LogManager.getLogger(LayerSimilarity.class);
}
