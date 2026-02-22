"""
EDA and BVP feature extraction from phase-segmented CSV files.
Reads from the pipeline's phase_segmented output and writes to etl/features_extracted.
"""
import os
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)

import neurokit2 as nk

# Define EDA and BVP features
eda_features = ['scl_mean', 'scl_std', 'phasic_mean', 'scr_count', 'scr_amp_mean', 'signal_quality']
bvp_features = ['R-R_Intervals', 'SDNN', 'RMSSD', 'HR', 'pNN50', 'Poincare_SD1', 'Poincare_SD2']

features = eda_features + bvp_features
cols = ['SubjectID'] + features + ['Phase', 'Intervention']
eda_features_length = len(eda_features) + 1
bvp_features_length = len(bvp_features) + eda_features_length


def get_paths():
    """Resolve project root and paths (aligned with transform.py)."""
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())
    base_etl = os.path.join(project_root, "etl")
    root_dir = os.path.join(base_etl, "phase_segmented")
    output_dir = os.path.join(base_etl, "features_extracted")
    output_file = os.path.join(output_dir, "feature_extracted.csv")
    return project_root, root_dir, output_dir, output_file


def get_subject_ids(path):
    if os.path.exists(path):
        return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
    return []


def extract_eda_features(eda_signal: np.ndarray, sampling_rate: int = 64):
    if not isinstance(eda_signal, np.ndarray):
        raise ValueError("Input must be a numpy array")
    if len(eda_signal) <= 15:
        return [np.nan] * 6
    try:
        eda_normalized = (eda_signal - np.mean(eda_signal)) / np.std(eda_signal)
        signals, info = nk.eda_process(eda_normalized, sampling_rate=sampling_rate)
        eda_tonic = signals["EDA_Tonic"]
        eda_phasic = signals["EDA_Phasic"]
        scr_amps = info["SCR_Amplitude"]
        scr_count = len(info["SCR_Peaks"]) if info["SCR_Peaks"] is not None else 0
        eda_features_dict = {
            'scl_mean': np.mean(eda_tonic),
            'scl_std': np.std(eda_tonic),
            'phasic_mean': np.mean(eda_phasic),
            'scr_count': scr_count,
            'scr_amp_mean': np.mean(scr_amps) if scr_count > 0 else 0,
            'signal_quality': np.var(eda_normalized),
        }
        return list(eda_features_dict.values())
    except Exception as e:
        raise RuntimeError(f"Processing failed: {str(e)}")


def normalize_bvp(bvp_signal: np.ndarray):
    baseline = np.median(bvp_signal)
    corrected = bvp_signal - baseline
    return (corrected - np.min(corrected)) / (np.max(corrected) - np.min(corrected) + 1e-8)


def extract_bvp_features(bvp_signal: np.ndarray, sampling_rate: int = 64):
    if not isinstance(bvp_signal, np.ndarray):
        raise ValueError("Input must be a numpy array")
    if len(bvp_signal) < 100:
        return [np.nan] * 7
    try:
        bvp_normalized = normalize_bvp(bvp_signal)
        bvp_cleaned = nk.ppg_clean(bvp_normalized, sampling_rate=sampling_rate)
        info = nk.ppg_findpeaks(bvp_cleaned, sampling_rate=sampling_rate)
        peaks = info["PPG_Peaks"]
        rri = np.diff(peaks) / sampling_rate
        if len(rri) < 2:
            return [np.nan] * 7
        sdnn = np.nanstd(rri, ddof=1)
        rmssd = np.sqrt(np.mean(np.diff(rri) ** 2))
        hr = 60 / np.mean(rri)
        diff_rri = np.abs(np.diff(rri))
        pnn50 = np.sum(diff_rri > 0.05) / len(diff_rri) * 100
        rri_n = rri[:-1]
        rri_plus = rri[1:]
        sd1 = np.std((rri_n - rri_plus) / np.sqrt(2), ddof=1)
        sd2 = np.std((rri_n + rri_plus) / np.sqrt(2), ddof=1)
        # Store scalar for R-R (mean) so DataFrame row is valid
        bvp_features_list = [
            np.mean(rri),
            sdnn,
            rmssd,
            hr,
            pnn50,
            sd1,
            sd2,
        ]
        return bvp_features_list
    except Exception as e:
        raise RuntimeError(f"BVP processing error: {str(e)}")


def extract_features(folder_path, subject_id, control_subjects, breathing_subjects, raga_subjects, features_df):
    if subject_id in control_subjects:
        intervention = 'Control'
    elif subject_id in breathing_subjects:
        intervention = 'Breathing'
    elif subject_id in raga_subjects:
        intervention = 'Raga'
    else:
        intervention = 'Unknown'

    print("Processing : " + subject_id)
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv') and (filename.startswith('eda_') or filename.startswith('bvp_')):
            file_path = os.path.join(folder_path, filename)
            condition = None
            if 'baseline' in filename:
                condition = 'baseline'
            elif 'intervention' in filename:
                condition = 'intervention'
            elif 'test' in filename:
                condition = 'test'
            elif 'rest' in filename:
                condition = 'rest'

            if condition is not None:
                df = pd.read_csv(file_path)
                row = [subject_id] + [np.nan] * len(features) + [condition] + [intervention]

                if filename.startswith('eda_'):
                    eda_signal = df['eda'].values
                    eda_features_values = extract_eda_features(eda_signal)
                    row[1:eda_features_length] = eda_features_values
                elif filename.startswith('bvp_'):
                    bvp_signal = df['bvp'].values
                    bvp_features_values = extract_bvp_features(bvp_signal)
                    row[eda_features_length:bvp_features_length] = bvp_features_values

                features_df.loc[len(features_df)] = row.copy()


def run_feature_extraction(root_dir=None, output_path=None):
    """
    Iterate through phase_segmented groups and subjects, extract EDA/BVP features.
    """
    project_root, default_root, output_dir, default_output = get_paths()
    root_dir = root_dir or default_root
    output_path = output_path or default_output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    control_path = os.path.join(root_dir, 'Control')
    breathing_path = os.path.join(root_dir, 'Breathing')
    raga_path = os.path.join(root_dir, 'Raga')

    control_subjects = get_subject_ids(control_path)
    breathing_subjects = get_subject_ids(breathing_path)
    raga_subjects = get_subject_ids(raga_path)

    print(f"Detected Control Subjects: {control_subjects}")
    print(f"Detected Breathing Subjects: {breathing_subjects}")
    print(f"Detected Raga Subjects: {raga_subjects}")

    features_df = pd.DataFrame(columns=cols)

    for parent_dir in [control_path, breathing_path, raga_path]:
        if not os.path.exists(parent_dir):
            print(f"Skipping missing path: {parent_dir}")
            continue
        for folder in os.listdir(parent_dir):
            folder_path = os.path.join(parent_dir, folder)
            if os.path.isdir(folder_path):
                try:
                    extract_features(
                        folder_path, folder,
                        control_subjects, breathing_subjects, raga_subjects,
                        features_df
                    )
                except Exception as e:
                    print(f"Error processing subject {folder}: {e}")

    if features_df.empty:
        print("No features extracted.")
        return None

    features_df.to_csv(output_path, index=False)
    print(f"Features saved to {output_path}")
    return output_path


def main():
    run_feature_extraction()


if __name__ == "__main__":
    main()
