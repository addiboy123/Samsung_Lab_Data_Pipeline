"""
EEG feature extraction from phase-segmented CSV files.
Reads from the pipeline's phase_segmented output and writes to etl/features_extracted.
"""
import os
import glob
import pandas as pd
import numpy as np
import antropy as ant
from scipy import signal
from warnings import simplefilter

# Suppress FutureWarning from pandas
simplefilter(action="ignore", category=FutureWarning)


# -----------------------------------------------------------------------------
# --- 1. CONFIGURATION ---
# -----------------------------------------------------------------------------

# EEG signal parameters
SAMPLING_RATE = 500  # Hz; change if your data differs
WINDOW_SECONDS = 60  # Duration of each window for feature calculation in seconds

# Frequency band definitions
BANDS = {
    'alpha': (8, 13),
    'beta': (13, 30)
}

# Channels of interest
CHANNELS_OF_INTEREST = ['EEG.F3', 'EEG.F4']


# -----------------------------------------------------------------------------
# --- 2. FEATURE CALCULATION FUNCTIONS ---
# -----------------------------------------------------------------------------

def bandpass_filter(data, lowcut=1.0, highcut=50.0, fs=SAMPLING_RATE, order=5):
    """Applies a Butterworth bandpass filter to the data."""
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = signal.butter(order, [low, high], btype='band')
    y = signal.lfilter(b, a, data)
    return y


def calculate_band_power(data, fs=SAMPLING_RATE):
    """Calculates the power in alpha and beta bands using Welch's method."""
    win = fs * 2  # 2-second window
    freqs, psd = signal.welch(data, fs, nperseg=win)

    alpha_power = np.trapz(psd[(freqs >= BANDS['alpha'][0]) & (freqs <= BANDS['alpha'][1])])
    beta_power = np.trapz(psd[(freqs >= BANDS['beta'][0]) & (freqs <= BANDS['beta'][1])])

    return alpha_power, beta_power


def extract_features_from_file(file_path, phase_name):
    """
    Extracts features from a single EEG file using a 60-second windowing approach.
    Features are calculated for each window, then averaged across all windows.
    """
    try:
        df = pd.read_csv(file_path)
        if not all(ch in df.columns for ch in CHANNELS_OF_INTEREST):
            print(f"  - Skipping {os.path.basename(file_path)}: Missing required channels.")
            return None

        if phase_name == 'baseline':
            duration_seconds = 60
        else:
            duration_seconds = 240  # 4 minutes (control, intervention, test)

        total_samples = int(duration_seconds * SAMPLING_RATE)

        if len(df) < total_samples:
            print(f"  - Warning in {os.path.basename(file_path)}: Not enough data for full duration. Using available data.")
            total_samples = len(df)

        data_f3 = df['EEG.F3'].iloc[:total_samples].to_numpy()
        data_f4 = df['EEG.F4'].iloc[:total_samples].to_numpy()

        window_samples = int(WINDOW_SECONDS * SAMPLING_RATE)
        num_windows = total_samples // window_samples

        if num_windows == 0:
            print(f"  - Skipping {os.path.basename(file_path)}: Not enough data for a single {WINDOW_SECONDS}s window.")
            return None

        all_alpha_asymmetry, all_hfd_f3, all_hfd_f4, all_beta_alpha_f3, all_beta_alpha_f4 = [], [], [], [], []

        for i in range(num_windows):
            start_idx = i * window_samples
            end_idx = start_idx + window_samples

            win_f3 = data_f3[start_idx:end_idx]
            win_f4 = data_f4[start_idx:end_idx]

            filt_f3 = bandpass_filter(win_f3)
            filt_f4 = bandpass_filter(win_f4)

            alpha_f3, beta_f3 = calculate_band_power(filt_f3)
            alpha_f4, beta_f4 = calculate_band_power(filt_f4)

            ratio_f3 = beta_f3 / alpha_f3 if alpha_f3 > 0 else 0
            ratio_f4 = beta_f4 / alpha_f4 if alpha_f4 > 0 else 0
            all_beta_alpha_f3.append(ratio_f3)
            all_beta_alpha_f4.append(ratio_f4)

            asymmetry = np.log1p(alpha_f4) - np.log1p(alpha_f3) if alpha_f3 > 0 and alpha_f4 > 0 else 0
            all_alpha_asymmetry.append(asymmetry)

            hfd_f3 = ant.higuchi_fd(filt_f3)
            hfd_f4 = ant.higuchi_fd(filt_f4)
            all_hfd_f3.append(hfd_f3)
            all_hfd_f4.append(hfd_f4)

        features = {
            'Alpha_Asymmetry': np.mean(all_alpha_asymmetry),
            'HFD_F3': np.mean(all_hfd_f3),
            'HFD_F4': np.mean(all_hfd_f4),
            'Beta_Alpha_Ratio_F3': np.mean(all_beta_alpha_f3),
            'Beta_Alpha_Ratio_F4': np.mean(all_beta_alpha_f4),
        }
        return features

    except Exception as e:
        print(f"  - Error processing file {file_path}: {e}")
        return None


# -----------------------------------------------------------------------------
# --- 3. MAIN SCRIPT LOGIC ---
# -----------------------------------------------------------------------------

def get_paths():
    """Resolve project root and paths (aligned with transform.py)."""
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())
    base_etl = os.path.join(project_root, "etl")
    base_path = os.path.join(base_etl, "phase_segmented")
    output_dir = os.path.join(base_etl, "features_extracted")
    output_filename = os.path.join(output_dir, "eeg_features_output.csv")
    return project_root, base_path, output_dir, output_filename


def run_feature_extraction(base_path=None, output_path=None):
    """
    Iterate through phase_segmented groups and subjects, extract EEG features.
    If base_path or output_path are None, they are derived from project root.
    """
    project_root, default_base, output_dir, default_output = get_paths()
    base_path = base_path or default_base
    output_path = output_path or default_output
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    # Pipeline uses Control, Raga, Breathing; normalize to lowercase for output
    groups = ['Control', 'Raga', 'Breathing']
    all_features_data = []

    if not os.path.exists(base_path):
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"!!! ERROR: base_path does not exist: {base_path}")
        print("!!! Run transform_data first to produce phase_segmented data.")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return None

    print("Starting EEG feature extraction...")

    for group in groups:
        group_path = os.path.join(base_path, group)
        if not os.path.isdir(group_path):
            continue
        subject_folders = sorted(
            d for d in glob.glob(os.path.join(group_path, "*"))
            if os.path.isdir(d)
        )

        print(f"\nProcessing Group: {group} ({len(subject_folders)} subjects found)")

        for subject_path in subject_folders:
            subject_id = os.path.basename(subject_path)
            print(f"- Processing Subject: {subject_id}")

            csv_files = glob.glob(os.path.join(subject_path, "*.csv"))

            for file_path in csv_files:
                file_name = os.path.basename(file_path)
                phase_name = file_name.replace(".csv", "").lower()
                if "setup" in phase_name:
                    continue

                features = extract_features_from_file(file_path, phase_name)

                if features:
                    result_row = {
                        'SubjectID': subject_id,
                        'Phase': phase_name.split('_')[-1],
                        'Group': group.lower(),
                        **features
                    }
                    all_features_data.append(result_row)

    if not all_features_data:
        print("\nNo features extracted (no EEG files with required channels found).")
        return None

    output_df = pd.DataFrame(all_features_data)

    try:
        output_df.to_csv(output_path, index=False)
        print(f"\n✅ Success! All features saved to '{output_path}'")
        return output_path
    except Exception as e:
        print(f"\n❌ Error saving file: {e}")
        return None


def main():
    """Entry point for standalone or DAG usage."""
    run_feature_extraction()


if __name__ == "__main__":
    main()
