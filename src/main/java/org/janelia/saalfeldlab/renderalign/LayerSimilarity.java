package org.janelia.saalfeldlab.renderalign;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import mpicbg.imagefeatures.Feature;
import mpicbg.models.PointMatch;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.janelia.alignment.match.CanvasFeatureMatchResult;
import org.janelia.alignment.match.CanvasFeatureMatcher;

/**
 * Calculates the similarity between two layers based on supplied features.
 *
 * @author Stephan Saalfeld <saalfelds@janelia.hhmi.org>
 */
public class LayerSimilarity implements Serializable, Comparable<LayerSimilarity> {

    private Double z1;
    private Double z2;
    private CanvasFeatureMatcher canvasFeatureMatcher;
    private CanvasFeatureMatchResult canvasFeatureMatchResult;

    public LayerSimilarity(final Double z1,
                           final Double z2,
                           final CanvasFeatureMatcher canvasFeatureMatcher) {
        this.z1 = z1;
        this.z2 = z2;
        this.canvasFeatureMatcher = canvasFeatureMatcher;
    }

    public Double getZ1() {
        return z1;
    }

    public Double getZ2() {
        return z2;
    }

    public Double getInlierRatio() {
        return canvasFeatureMatchResult.getInlierRatio();
    }

    public List<PointMatch> getInliers() {
    	return canvasFeatureMatchResult.getInlierPointMatchList();
    }

    public boolean isModelFound() {
        return canvasFeatureMatchResult.isModelFound();
    }

    @Override
    public String toString() {
    	return "{\"z1\": " + getZ1() +
               ", \"z2\": " + getZ2() +
               ", \"inlierRatio\": " + getInlierRatio() +
               ", \"modelFound\": " + isModelFound() + "}";
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
    public void calculateInlierRatio(final Map<Double, LayerFeatures> zToFeaturesMap)
            throws IllegalArgumentException {

        final List<Feature> features1 = getFeatureList(z1, zToFeaturesMap);
        final List<Feature> features2 = getFeatureList(z2, zToFeaturesMap);

        canvasFeatureMatchResult = canvasFeatureMatcher.deriveMatchResult(features1, features2);
    }

    private List<Feature> getFeatureList(final Double z,
                                         final Map<Double, LayerFeatures> zToFeaturesMap)
            throws IllegalArgumentException {

        final LayerFeatures layerFeatures = zToFeaturesMap.get(z);

        final List<Feature> featureList = layerFeatures.getPersistedFeatureList();

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
