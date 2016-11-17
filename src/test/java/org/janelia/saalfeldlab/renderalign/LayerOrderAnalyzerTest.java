package org.janelia.saalfeldlab.renderalign;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;

import org.janelia.alignment.match.CanvasFeatureMatcher;
import org.janelia.alignment.match.ModelType;
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
                "http://tem-services.int.janelia.org:8080/render-ws/v1/owner/flyTEM/project/FAFB00/stack/v13_montage");

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
            final LayerFeatures layerFeatures = new LayerFeatures(z, null, null, montageFile, featureListFile, null);
            layerFeatures.loadMontageAndExtractFeatures(false,
                                                        new FloatArray2DSIFT.Param(),
                                                        0.5,
                                                        0.85,
                                                        false);
            zToFeaturesMap.put(z, layerFeatures);
        }

        final CanvasFeatureMatcher canvasFeatureMatcher = new CanvasFeatureMatcher(0.92f,
                                                                                   ModelType.AFFINE,
                                                                                   1000,
                                                                                   20f,
                                                                                   0.0f,
                                                                                   10,
                                                                                   3,
                                                                                   null,
                                                                                   true);
        for (int i = 0; i < n; i++) {
            final Double z1 = zValues.get(i);
            for (int k = i + 1; k < n; k++) {
                final Double z2 = zValues.get(k);
                final LayerSimilarity layerSimilarity = new LayerSimilarity(z1, z2, canvasFeatureMatcher);
                layerSimilarity.calculateInlierRatio(zToFeaturesMap);
//                System.out.println("model is " + layerSimilarity.getModel());
            }
        }

    }

    @Test
    public void testClip()
            throws IOException {

        final Double z = 1.0;
        //final String renderUrl =
                //"http://tem-services.int.janelia.org:8080/render-ws/v1/owner/flyTEM/project/FAFB00/stack/v13_montage/z/" +
                //z + "/render-parameters?scale=0.8";

        final String renderUrl = "http://renderer-dev:8080/render-ws/v1/owner/flyTEM/project/FAFB00/" +
                                 "stack/v12_acquire_merged/tile/150311140241101032.3334.0" +
                                 "/render-parameters?excludeMask=true&normalizeForMatching=true&width=2760&height=2330&scale=0.03";

        final LayerFeatures layerFeatures = new LayerFeatures(1.0, renderUrl, null, null, null, null);
        final List<Feature> featureList =
                layerFeatures.loadMontageAndExtractFeatures(false,
                                                            new FloatArray2DSIFT.Param(),
                                                            0.5,
                                                            0.85,
                                                            true);
        System.out.println(layerFeatures);
        printFeatureList("no clip", featureList);

        final LayerFeatures clippedLayerFeatures = new LayerFeatures(1.0, renderUrl, null, null, null, 0.6);
        final List<Feature> clippedFeatureList =
                clippedLayerFeatures.loadMontageAndExtractFeatures(false,
                                                                   new FloatArray2DSIFT.Param(),
                                                                   0.5,
                                                                   0.85,
                                                                   true);

        System.out.println(clippedLayerFeatures);
        printFeatureList("with clip", clippedFeatureList);
    }

    private void printFeatureList(final String context,
                                  final List<Feature> featureList) {
        int count = 0;
        for (final Feature feature : featureList) {
            System.out.println(context + ": (" + feature.location[0] + ", " + feature.location[1] + ")");
            count++;

            if (count > 20) {
                break;
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
