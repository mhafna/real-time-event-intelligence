import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

DATA_PATH = Path("data/anomaly_features_real.csv")
JOBLIB_PATH = Path("ml/models/isolation_forest_model.joblib")
JSON_PATH = Path("ml/models/isolation_forest_model.json")

# ---------------------------------------------------------
# Load original sklearn model
# ---------------------------------------------------------
bundle = joblib.load(JOBLIB_PATH)
sk_model = bundle["model"]
features = bundle["features"]

df = pd.read_csv(DATA_PATH)

# sklearn converts IsolationForest scoring input to float32.
X = df[features].to_numpy(dtype=np.float32)

# Original sklearn results
sk_decision = sk_model.decision_function(df[features])
sk_prediction = sk_model.predict(df[features])

# ---------------------------------------------------------
# Load portable JSON model
# ---------------------------------------------------------
with open(JSON_PATH, "r", encoding="utf-8") as f:
    portable = json.load(f)

trees = portable["trees"]
normalization_c = float(portable["normalization_c"])
offset = float(portable["offset"])
n_trees = len(trees)

json_decisions = []
json_predictions = []

# ---------------------------------------------------------
# Reproduce Isolation Forest inference from exported trees
# ---------------------------------------------------------
for row in X:
    total_depth = 0.0

    for tree in trees:
        node = 0

        children_left = tree["children_left"]
        children_right = tree["children_right"]
        split_features = tree["feature"]
        thresholds = tree["threshold"]
        feature_indices = tree["feature_indices"]

        while children_left[node] != children_right[node]:
            local_feature = split_features[node]
            original_feature = feature_indices[local_feature]

            if row[original_feature] <= thresholds[node]:
                node = children_left[node]
            else:
                node = children_right[node]

        total_depth += (
            tree["decision_path_lengths"][node]
            + tree["average_path_lengths"][node]
            - 1.0
        )

    raw_if_score = 2.0 ** (
        -total_depth / (n_trees * normalization_c)
    )

    # sklearn score_samples() is the negative of the
    # original Isolation Forest anomaly score.
    score_samples = -raw_if_score

    decision = score_samples - offset

    prediction = -1 if decision < 0.0 else 1

    json_decisions.append(decision)
    json_predictions.append(prediction)

json_decisions = np.asarray(json_decisions)
json_predictions = np.asarray(json_predictions)

# ---------------------------------------------------------
# Compare
# ---------------------------------------------------------
abs_diff = np.abs(sk_decision - json_decisions)

print("Rows checked:", len(df))
print("Trees checked:", n_trees)
print()
print("Maximum decision-score difference:", abs_diff.max())
print("Mean decision-score difference:", abs_diff.mean())
print(
    "Prediction matches:",
    int(np.sum(sk_prediction == json_predictions)),
    "/",
    len(df),
)

print()

if np.all(sk_prediction == json_predictions):
    print("PASS: All NORMAL / ANOMALY predictions match.")
else:
    print("FAIL: Prediction mismatch detected.")

if abs_diff.max() < 1e-10:
    print("PASS: Numerical scores match to very high precision.")
elif abs_diff.max() < 1e-6:
    print("PASS: Numerical scores match within floating-point tolerance.")
else:
    print("WARNING: Score difference needs investigation.")

# Show top 6 for visual confirmation
result = pd.DataFrame({
    "msn": df["msn"],
    "window_start": df["window_start"],
    "sklearn_decision": sk_decision,
    "json_decision": json_decisions,
    "difference": abs_diff,
    "sklearn_prediction": sk_prediction,
    "json_prediction": json_predictions,
})

print("\nTop 6 most anomalous according to JSON model:")
print(
    result
    .sort_values("json_decision")
    .head(6)
    .to_string(index=False)
)
