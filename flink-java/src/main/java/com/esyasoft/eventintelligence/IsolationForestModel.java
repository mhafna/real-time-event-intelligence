package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.List;

public class IsolationForestModel {

    public String model_type;
    public String sklearn_version;
    public List<String> features;
    public List<String> excluded_constant_features;
    public double contamination;
    public int n_estimators;
    public int max_samples;
    public double offset;
    public double normalization_c;
    public List<TreeModel> trees;

    public static class TreeModel {
        public int tree_index;
        public int[] feature_indices;
        public int[] children_left;
        public int[] children_right;
        public int[] feature;
        public double[] threshold;
        public double[] decision_path_lengths;
        public double[] average_path_lengths;
    }

    public static IsolationForestModel loadFromResource(String resourcePath)
            throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        try (InputStream input =
                     IsolationForestModel.class
                             .getClassLoader()
                             .getResourceAsStream(resourcePath)) {

            if (input == null) {
                throw new IllegalArgumentException(
                        "Isolation Forest model resource not found: "
                                + resourcePath
                );
            }

            return mapper.readValue(
                    input,
                    IsolationForestModel.class
            );
        }
    }

    public double decisionFunction(double[] inputFeatures) {

        if (inputFeatures.length != features.size()) {
            throw new IllegalArgumentException(
                    "Expected " + features.size()
                            + " features but received "
                            + inputFeatures.length
            );
        }

        double totalDepth = 0.0;

        for (TreeModel tree : trees) {

            int node = 0;

            while (tree.children_left[node]
                    != tree.children_right[node]) {

                int localFeature =
                        tree.feature[node];

                int originalFeature =
                        tree.feature_indices[localFeature];

                // sklearn IsolationForest scoring uses float32 input.
                float value =
                        (float) inputFeatures[originalFeature];

                double threshold =
                        tree.threshold[node];

                if (value <= threshold) {
                    node = tree.children_left[node];
                } else {
                    node = tree.children_right[node];
                }
            }

            totalDepth +=
                    tree.decision_path_lengths[node]
                            + tree.average_path_lengths[node]
                            - 1.0;
        }

        double denominator =
                trees.size() * normalization_c;

        double rawIsolationScore =
                Math.pow(
                        2.0,
                        -totalDepth / denominator
                );

        // sklearn score_samples() returns the negative
        // of the original Isolation Forest score.
        double scoreSamples =
                -rawIsolationScore;

        // sklearn decision_function()
        return scoreSamples - offset;
    }

    public int predict(double[] inputFeatures) {
        return decisionFunction(inputFeatures) < 0.0
                ? -1
                : 1;
    }

    public String predictLabel(double[] inputFeatures) {
        return predict(inputFeatures) == -1
                ? "ANOMALY"
                : "NORMAL";
    }

    public double anomalyScore(double[] inputFeatures) {
        // Our dashboard convention:
        // larger positive value = more anomalous.
        return -decisionFunction(inputFeatures);
    }
}
