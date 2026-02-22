"""
Plot extracted features by Phase and Group (bar plots with mean ± std).
Reads from pipeline features CSV and saves plots under etl/plots.
"""
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def cap_outliers_iqr(series):
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    return series.clip(Q1 - 1.5 * IQR, Q3 + 1.5 * IQR)


def get_paths():
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())
    base_etl = os.path.join(project_root, "etl")
    features_path = os.path.join(base_etl, "features_extracted", "feature_extracted.csv")
    plots_dir = os.path.join(base_etl, "plots")
    return features_path, plots_dir


def run_error_plotting(features_csv_path=None, plots_dir=None):
    """Load features CSV and create bar plots per feature (one fig per feature, one subplot per group)."""
    default_csv, default_plots = get_paths()
    features_csv_path = features_csv_path or default_csv
    plots_dir = plots_dir or default_plots
    os.makedirs(plots_dir, exist_ok=True)

    if not os.path.isfile(features_csv_path):
        print(f"Features file not found: {features_csv_path}")
        return None

    df = pd.read_csv(features_csv_path)
    df.drop(columns=['SubjectID'], inplace=True, errors='ignore')

    # Support both 'Intervention' (feature_extraction output) and 'group'
    group_col = 'Intervention' if 'Intervention' in df.columns else 'group'
    if group_col not in df.columns:
        print("No group/Intervention column in CSV.")
        return None

    groups = ['Control', 'Raga', 'Breathing']
    all_phases = ['baseline', 'intervention', 'test']
    feature_columns = [col for col in df.columns if col not in [group_col, 'Phase']]

    # Get global y-limits for each feature
    y_limits = {}
    for feature in feature_columns:
        all_values = pd.Series(dtype='float64')
        for group in groups:
            phases = all_phases if group != 'Control' else ['baseline', 'test']
            subset = df[(df[group_col] == group) & (df['Phase'].isin(phases))].copy()
            if not subset.empty:
                capped = subset.groupby('Phase')[feature].transform(cap_outliers_iqr)
                all_values = pd.concat([all_values, capped])
        if len(all_values) > 0 and np.ptp(all_values) > 0:
            y_min = all_values.min() - 0.1 * np.ptp(all_values)
            y_max = all_values.max() + 0.1 * np.ptp(all_values)
            y_limits[feature] = (y_min, y_max)
        else:
            y_limits[feature] = (0, 1)

    # Generate plots
    for feature in feature_columns:
        fig, axs = plt.subplots(1, len(groups), figsize=(18, 5), sharey=True)
        fig.suptitle(f'Feature: {feature}', y=1.02, fontsize=16)

        if len(groups) == 1:
            axs = [axs]

        for idx, group in enumerate(groups):
            phases = all_phases if group != 'Control' else ['baseline', 'test']
            subset = df[(df[group_col] == group) & (df['Phase'].isin(phases))].copy()
            subset[feature] = subset.groupby('Phase')[feature].transform(cap_outliers_iqr)

            means = subset.groupby('Phase')[feature].mean().reindex(phases)
            stds = subset.groupby('Phase')[feature].std().reindex(phases)
            x = np.arange(len(phases))

            axs[idx].bar(x, means, yerr=stds, capsize=8, color='gray',
                         edgecolor='black', alpha=0.85)
            axs[idx].plot(x, means.values, color='red', linestyle='--', marker='o')
            axs[idx].set_xticks(x)
            axs[idx].set_xticklabels(phases)
            axs[idx].set_title(group, fontsize=12)
            axs[idx].grid(axis='y', linestyle='--', alpha=0.6)
            axs[idx].set_ylim(y_limits[feature])

        plt.tight_layout()
        safe_name = feature.replace(" ", "_").replace("/", "_")
        out_path = os.path.join(plots_dir, f"feature_{safe_name}.png")
        plt.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"Saved: {out_path}")

    print(f"Plots saved to {plots_dir}")
    return plots_dir


def main():
    run_error_plotting()


if __name__ == "__main__":
    main()
