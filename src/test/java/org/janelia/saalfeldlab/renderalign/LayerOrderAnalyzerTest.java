package org.janelia.saalfeldlab.renderalign;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mpicbg.imagefeatures.Feature;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for {@link LayerOrderAnalyzer}.
 * These tests take too long to run regularly, so they have been marked with the @Ignore annotation.
 *
 * @author Eric Trautman
 */
@Ignore
public class LayerOrderAnalyzerTest {

    @Test
    public void tesGetZValues() throws Exception {
        List<Double> zValues = LayerOrderAnalyzer.getZValues(
                "http://tem-services.int.janelia.org:8080/render-ws/v1/owner/flyTEM/project/FAFB00/stack/v8_montage");

        Assert.assertTrue("missing z values", zValues.size() > 10);
    }

    @Test
    public void testRatios() {

        final int n = 5;

        final List<Double> zValues = new ArrayList<>();
        final Map<Double, List<Feature>> zToFeaturesMap = new HashMap<>();

        for (int i = 0; i < n; i++) {
            final Double z = i + 2050.0;
            zValues.add(z);
            final LayerFeatures layerFeatures = new LayerFeatures(z);
            layerFeatures.loadMontage("not-used", new File("src/test/resources/montage/" + z + ".png"), false);
            layerFeatures.extractFeatures(LayerFeatures.DEFAULT_SIFT_PARAMETERS);
            zToFeaturesMap.put(z, layerFeatures.getFeatureList());
        }

        for (int i = 0; i < n; i++) {
            final Double z1 = zValues.get(i);
            for (int k = i + 1; k < n; k++) {
                final Double z2 = zValues.get(k);
                final LayerSimilarity layerSimilarity = new LayerSimilarity(z1, z2);
                layerSimilarity.calculateInlierRatio(zToFeaturesMap);
//                System.out.println("model is " + layerSimilarity.getModel());
            }
        }

    }
}
