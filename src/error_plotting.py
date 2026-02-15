"""
Plot EEG features by Phase and Group (mean ± SD).
Reads from pipeline features CSV and saves plots under etl/plots.
"""
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for Airflow/server
import matplotlib.pyplot as plt


FEATURE_COLUMNS = [
    'Alpha_Asymmetry',
    'HFD_F3', 'HFD_F4',
    'Beta_Alpha_Ratio_F3', 'Beta_Alpha_Ratio_F4'
]
PHASE_ORDER = ["baseline", "control", "intervention", "test"]
GROUPS = ["control", "raga", "breathing"]


def get_paths():
    """Resolve project root and paths (aligned with feature_extraction.py)."""
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())
    base_etl = os.path.join(project_root, "etl")
    features_path = os.path.join(base_etl, "features_extracted", "eeg_features_output.csv")
    plots_dir = os.path.join(base_etl, "plots")
    return project_root, features_path, plots_dir


def run_error_plotting(features_csv_path=None, plots_dir=None):
    """
    Load features CSV, plot each feature by Phase and Group, save figures.
    If paths are None, they are derived from project root.
    """
    _, default_csv, default_plots = get_paths()
    features_csv_path = features_csv_path or default_csv
    plots_dir = plots_dir or default_plots
    os.makedirs(plots_dir, exist_ok=True)

    if not os.path.isfile(features_csv_path):
        print(f"!!! ERROR: Features file not found: {features_csv_path}")
        print("Run feature_extraction first.")
        return None

    df = pd.read_csv(features_csv_path)
    df["Phase"] = pd.Categorical(df["Phase"], categories=PHASE_ORDER, ordered=True)

    for feature in FEATURE_COLUMNS:
        if feature not in df.columns:
            print(f"  - Skipping {feature}: column not in CSV.")
            continue

        fig, axes = plt.subplots(1, len(GROUPS), figsize=(15, 5), sharey=False)
        axes = np.atleast_1d(axes)  # single group -> axes is scalar otherwise
        fig.suptitle(f"Feature: {feature}", fontsize=16, y=1.05)

        for i, group in enumerate(GROUPS):
            ax = axes[i]
            group_df = df[df["Group"] == group]

            mean_values = group_df.groupby("Phase")[feature].mean().reindex(PHASE_ORDER)
            std_values = group_df.groupby("Phase")[feature].std().reindex(PHASE_ORDER)

            ax.bar(PHASE_ORDER, mean_values, yerr=std_values, color="gray", alpha=0.8, capsize=5)
            ax.plot(PHASE_ORDER, mean_values, "o--", color="red", linewidth=1.5)

            ax.set_title(group.capitalize(), fontsize=13)
            ax.set_xlabel("Phase")
            if i == 0:
                ax.set_ylabel("Mean ± SD")
            ax.grid(True, linestyle="--", alpha=0.5)

            if feature == "Alpha_Asymmetry":
                ymin = mean_values.min() - abs(mean_values.min()) * 1.5
                ymax = mean_values.max() + abs(mean_values.max()) * 1.5
                ax.set_ylim(ymin, ymax)
            else:
                ymin = max(0, mean_values.min() - std_values.max() * 1.5)
                ymax = mean_values.max() + std_values.max() * 1.5
                ax.set_ylim(ymin, ymax)

        plt.tight_layout()
        safe_name = feature.replace(" ", "_").replace("/", "_")
        out_path = os.path.join(plots_dir, f"feature_{safe_name}.png")
        plt.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {out_path}")

    # Print Beta/Alpha summary to logs
    for feature in ["Beta_Alpha_Ratio_F3", "Beta_Alpha_Ratio_F4"]:
        if feature not in df.columns:
            continue
        mean_by_phase = df.groupby("Phase")[feature].mean()
        print(f"\n{feature} mean values by Phase:")
        print(mean_by_phase)

    print(f"\n✅ Plots saved to '{plots_dir}'")
    return plots_dir


def main():
    """Entry point for standalone or DAG usage."""
    run_error_plotting()


if __name__ == "__main__":
    main()
