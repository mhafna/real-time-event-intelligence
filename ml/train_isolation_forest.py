from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

DATA_PATH = Path("data/anomaly_features_real.csv")
MODEL_DIR = Path("ml/models")
OUTPUT_DIR = Path("ml/output")

ALL_FEATURES = [
    "event_count_15m",
    "avg_voltage_15m",
    "voltage_range_15m",
    "power_failure_count_15m",
]

MODEL_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Load real Flink-generated feature windows
df = pd.read_csv(DATA_PATH)

print(f"Loaded rows: {len(df)}")
print(f"Unique meters: {df['msn'].nunique()}")

# 2. Check for missing values
missing = df[ALL_FEATURES].isna().sum()
print("\nMissing values:")
print(missing.to_string())

# 3. Automatically exclude constant features for this training run
active_features = [
    col for col in ALL_FEATURES
    if df[col].nunique(dropna=True) > 1
]

excluded_features = [
    col for col in ALL_FEATURES
    if col not in active_features
]

print("\nFeatures used:")
for feature in active_features:
    print(f"  - {feature}")

print("\nExcluded constant features:")
for feature in excluded_features:
    print(f"  - {feature}")

X = df[active_features].copy()

# 4. Train Isolation Forest
model = IsolationForest(
    n_estimators=200,
    contamination=0.10,
    random_state=42,
)

model.fit(X)

# sklearn prediction:
#  1 = normal
# -1 = anomaly
df["if_prediction"] = model.predict(X)

# decision_function is larger for normal observations.
# Negating it makes larger values mean "more anomalous".
df["anomaly_score"] = -model.decision_function(X)

df["anomaly_label"] = df["if_prediction"].map({
    1: "NORMAL",
    -1: "ANOMALY"
})

# 5. Rank most unusual windows first
df = df.sort_values("anomaly_score", ascending=False).reset_index(drop=True)
df["anomaly_rank"] = range(1, len(df) + 1)

# 6. Save scored results
scored_path = OUTPUT_DIR / "isolation_forest_scored_windows.csv"
df.to_csv(scored_path, index=False)

# Save model together with feature metadata
model_path = MODEL_DIR / "isolation_forest_model.joblib"

joblib.dump(
    {
        "model": model,
        "features": active_features,
        "excluded_constant_features": excluded_features,
        "contamination": 0.10,
    },
    model_path,
)

# 7. Print summary
anomaly_count = (df["anomaly_label"] == "ANOMALY").sum()

print("\nTraining complete.")
print(f"Rows scored: {len(df)}")
print(f"Anomalies flagged: {anomaly_count}")
print(f"Model saved: {model_path}")
print(f"Scored data saved: {scored_path}")

print("\nTop 10 most anomalous windows:")
print(
    df[
        [
            "anomaly_rank",
            "msn",
            "window_start",
            "event_count_15m",
            "avg_voltage_15m",
            "voltage_range_15m",
            "power_failure_count_15m",
            "anomaly_score",
            "anomaly_label",
        ]
    ]
    .head(10)
    .to_string(index=False)
)
