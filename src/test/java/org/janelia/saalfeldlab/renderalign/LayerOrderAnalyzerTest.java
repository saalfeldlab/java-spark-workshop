package org.janelia.saalfeldlab.renderalign;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mpicbg.imagefeatures.FloatArray2DSIFT;

import org.junit.After;
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

    private Map<Double, LayerFeatures> zToFeaturesMap = new HashMap<>();

    @After
    public void tearDown() {
        for (LayerFeatures layerFeatures : zToFeaturesMap.values()) {
            final File featureListFile = layerFeatures.getFeatureListFile();
            if ((featureListFile != null) && featureListFile.exists()) {
                if (! featureListFile.delete()) {
                    System.out.println("failed to delete " + featureListFile.getAbsolutePath());
                }
            }
        }
    }

    @Test
    public void tesGetZValues() throws Exception {
        final List<Double> zValues = LayerOrderAnalyzer.getZValues(
                "http://tem-services.int.janelia.org:8080/render-ws/v1/owner/flyTEM/project/FAFB00/stack/v8_montage");

        Assert.assertTrue("missing z values", zValues.size() > 10);
    }

    @Test
    public void testRatios()
            throws IOException {

        final int n = 5;

        final List<Double> zValues = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            final Double z = i + 2050.0;
            zValues.add(z);
            final File montageFile = new File(getMontageFilePath(z));
            final File featureListFile = new File(getFeatureListFilePath(z));
            final LayerFeatures layerFeatures = new LayerFeatures(z, null, null, montageFile, featureListFile);
            layerFeatures.loadMontageAndExtractFeatures(false,
                                                        new FloatArray2DSIFT.Param(),
                                                        0.5,
                                                        0.85,
                                                        false);
            zToFeaturesMap.put(z, layerFeatures);
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

    private static String getMontageFilePath(Double z) {
        return "src/test/resources/montage/" + z + ".png";
    }

    private static String getFeatureListFilePath(Double z) {
        return "src/test/resources/montage/" + z + "_features.ser";
    }

}
