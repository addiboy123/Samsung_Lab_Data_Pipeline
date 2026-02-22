"""
Plot extracted features by Phase and Group (mean ± std errorbar plots).
Reads from pipeline features CSV and saves plots under etl/plots.
Only the bar/errorbar plotting logic; no other processing.
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
    """Load features CSV and create errorbar plots per feature (bar plots only)."""
    default_csv, default_plots = get_paths()
    features_csv_path = features_csv_path or default_csv
    plots_dir = plots_dir or default_plots
    os.makedirs(plots_dir, exist_ok=True)

    if not os.path.isfile(features_csv_path):
        print(f"Features file not found: {features_csv_path}")
        return None

    df = pd.read_csv(features_csv_path)
    df.drop(columns=['SubjectID'], inplace=True, errors='ignore')

    # Use 'Intervention' from feature_extraction output; fallback to 'group'
    group_col = 'Intervention' if 'Intervention' in df.columns else 'group'
    if group_col not in df.columns:
        print("No group/Intervention column in CSV.")
        return None

    groups = ['Control', 'Raga', 'Breathing']
    all_phases = ['baseline', 'intervention', 'test']
    feature_columns = [c for c in df.columns if c not in [group_col, 'Phase']]
    group_colors = {
        'Control': 'blue',
        'Raga': 'green',
        'Breathing': 'orange'
    }
    phase_to_x = {p: i for i, p in enumerate(all_phases)}

    # Global Y-limits per feature
    y_limits = {}
    for feature in feature_columns:
        all_values = []
        for group in groups:
            subset = df[df[group_col] == group].copy()
            if subset.empty:
                continue
            subset[feature] = subset.groupby('Phase')[feature].transform(cap_outliers_iqr)
            all_values.append(subset[feature])
        if all_values:
            combined = pd.concat(all_values)
            if len(combined) > 0 and np.ptp(combined) > 0:
                pad = 0.1 * np.ptp(combined)
                y_limits[feature] = (combined.min() - pad, combined.max() + pad)
            else:
                y_limits[feature] = (0, 1)
        else:
            y_limits[feature] = (0, 1)

    # Plot each feature (bar/errorbar plots only)
    for feature in feature_columns:
        plt.figure(figsize=(10, 6))
        plt.title(f'Feature: {feature}', fontsize=16)

        for group in groups:
            subset = df[df[group_col] == group].copy()
            if subset.empty:
                continue
            subset[feature] = subset.groupby('Phase')[feature].transform(cap_outliers_iqr)
            means = subset.groupby('Phase')[feature].mean()
            stds = subset.groupby('Phase')[feature].std()

            if group == 'Control':
                phases = ['baseline', 'test']
            else:
                phases = ['baseline', 'intervention', 'test']

            x = [phase_to_x[p] for p in phases]
            y_mean = [means[p] for p in phases]
            y_std = [stds[p] for p in phases]

            plt.errorbar(
                x,
                y_mean,
                yerr=y_std,
                fmt='-o',
                capsize=6,
                label=group,
                color=group_colors[group]
            )

        plt.xticks(list(phase_to_x.values()), all_phases)
        plt.ylim(y_limits.get(feature, (0, 1)))
        plt.xlabel('Phase')
        plt.ylabel('Value')
        plt.grid(axis='y', linestyle='--', alpha=0.6)
        plt.legend(title='Group')
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
