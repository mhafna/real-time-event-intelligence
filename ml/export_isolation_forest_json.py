import json
from pathlib import Path

import joblib
import numpy as np
import sklearn

INPUT_PATH = Path("ml/models/isolation_forest_model.joblib")
OUTPUT_PATH = Path("ml/models/isolation_forest_model.json")

payload = joblib.load(INPUT_PATH)
model = payload["model"]

max_samples = int(model.max_samples_)

if max_samples <= 1:
    normalization_c = 0.0
elif max_samples == 2:
    normalization_c = 1.0
else:
    normalization_c = (
        2.0 * (np.log(max_samples - 1.0) + np.euler_gamma)
        - 2.0 * (max_samples - 1.0) / max_samples
    )

export = {
    "model_type": "IsolationForest",
    "sklearn_version": sklearn.__version__,
    "features": payload["features"],
    "excluded_constant_features": payload.get(
        "excluded_constant_features", []
    ),
    "contamination": payload.get("contamination"),
    "n_estimators": len(model.estimators_),
    "max_samples": max_samples,
    "offset": float(model.offset_),
    "normalization_c": float(normalization_c),
    "trees": [],
}

for i, estimator in enumerate(model.estimators_):
    tree = estimator.tree_

    export["trees"].append({
        "tree_index": i,
        "feature_indices": [
            int(x) for x in model.estimators_features_[i]
        ],
        "children_left": tree.children_left.astype(int).tolist(),
        "children_right": tree.children_right.astype(int).tolist(),
        "feature": tree.feature.astype(int).tolist(),
        "threshold": tree.threshold.astype(float).tolist(),

        # These reproduce sklearn's path-length scoring directly.
        "decision_path_lengths": (
            np.asarray(model._decision_path_lengths[i])
            .astype(float)
            .tolist()
        ),
        "average_path_lengths": (
            np.asarray(model._average_path_length_per_tree[i])
            .astype(float)
            .tolist()
        ),
    })

OUTPUT_PATH.write_text(
    json.dumps(export, indent=2),
    encoding="utf-8",
)

print("Isolation Forest JSON export complete.")
print("Output:", OUTPUT_PATH)
print("Trees exported:", len(export["trees"]))
print("Features:", export["features"])
print("max_samples:", export["max_samples"])
print("normalization_c:", export["normalization_c"])
print("offset:", export["offset"])
