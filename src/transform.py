from utils.initialize import ETLInitializer
from utils.Avro_to_CSV import AvroProcessor
from utils.Segment_Division import CSVSegmenter
from utils.feature_extraction import run_feature_extraction
from utils.error_plotting import run_error_plotting
import os
from datetime import datetime

def transform_data():
    # --- Configuration ---
    START_DATE = "2026-01-14"
    END_DATE = "2026-02-18"
    
    # Dynamically get the project root path
    # Defaults to current directory if AIRFLOW_HOME is somehow not set
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())

    # --- Get today's date for storage ---
    today_date = datetime.now().strftime("%Y-%m-%d")

    # --- Robust Path Configuration ---
    
    # 1. The CSV is in the root
    METADATA_CSV = os.path.join(project_root, "Participants_Record.csv")
    
    # 2. Base ETL folder is also in the root
    # (Removed the '..' so it looks inside your project)
    BASE_ETL_PATH = os.path.join(project_root, "etl")
    
    # 3. Sub-directories
    # Note: Using 'tmp' inside the etl folder for intermediate processing
    UNPROCESSED_DIR = os.path.join(BASE_ETL_PATH, "unprocessed") 
    RAW_DIR         = os.path.join(BASE_ETL_PATH, "raw")
    CSV_TEMP_DIR    = os.path.join(BASE_ETL_PATH, "processed_csv")
    ORGANIZED_DIR   = os.path.join(BASE_ETL_PATH, "organized_data")
    SEGMENTED_DIR   = os.path.join(BASE_ETL_PATH, "phase_segmented")
    
    # 4. Features and plots directories with dated subdirectories
    FEATURES_DIR    = os.path.join(BASE_ETL_PATH, "features_extracted", today_date)
    PLOTS_DIR       = os.path.join(BASE_ETL_PATH, "plots", today_date)
    FEATURES_CSV    = os.path.join(FEATURES_DIR, "eeg_features_output.csv")

    # Log the paths to the Airflow UI so you can verify them in the logs
    print(f"Project Root: {project_root}")
    print(f"Loading Metadata from: {METADATA_CSV}")
    
    
    # --- STEP 0: Initialize & Dynamic Mapping ---
    print(f"Initializing data and fetching group mapping from {METADATA_CSV}...")
    initializer = ETLInitializer(METADATA_CSV, UNPROCESSED_DIR, RAW_DIR)
    
    # Dynamically generate the subject_mapping from CSV
    subject_mapping = initializer.get_group_mapping()
    active_dates = initializer.prepare_raw_data(START_DATE, END_DATE)

    if not active_dates:
        print("No data in this date range.")
        return

    # --- STEP 1: Avro to Organized CSV ---
    avro_proc = AvroProcessor(RAW_DIR, CSV_TEMP_DIR, ORGANIZED_DIR)
    for date_folder in active_dates:
        avro_proc.rename_avro_files(date_folder)
    
    avro_proc.process_avro_to_csv()
    # Now using the dynamic mapping
    avro_proc.organize_by_subject(subject_mapping)

    # --- STEP 2: Phase Segmentation ---
    # The rules (ratios) are still logic-based, but applied to groups found in CSV
    segment_rules = {
        'Control': (['baseline', 'test'], [1,5]),
        'Breathing': (['baseline', 'intervention', 'test'], [1, 5, 5]),
        'Raga': (['baseline', 'intervention', 'test'], [1, 5, 5])
    }

    for group, (exts, ratios) in segment_rules.items():
        group_input = os.path.join(ORGANIZED_DIR, group)
        group_output = os.path.join(SEGMENTED_DIR, group)
        
        if os.path.exists(group_input):
            print(f"Segmenting group: {group}")
            segmenter = CSVSegmenter(group_input, group_output, exts, ratios)
            segmenter.run_segmentation()

    print("--- SEGMENTATION COMPLETE ---")

    # --- STEP 3: Feature Extraction ---
    print(f"\nStarting Feature Extraction (output dir: {FEATURES_DIR})...")
    os.makedirs(FEATURES_DIR, exist_ok=True)
    features_path = run_feature_extraction(
        root_dir=SEGMENTED_DIR,
        output_path=FEATURES_CSV
    )
    
    if not features_path:
        print("❌ Feature extraction failed. Skipping error plotting.")
        print("--- ETL PARTIAL (feature extraction failed) ---")
        return SEGMENTED_DIR

    # --- STEP 4: Error Plotting ---
    print(f"\nStarting Error Plotting (output dir: {PLOTS_DIR})...")
    os.makedirs(PLOTS_DIR, exist_ok=True)
    plots_path = run_error_plotting(
        features_csv_path=features_path,
        plots_dir=PLOTS_DIR
    )

    print("\n--- ETL SUCCESSFUL ---")
    print(f"📊 Summary:")
    print(f"  - Segmented Data: {SEGMENTED_DIR}")
    print(f"  - Features CSV: {features_path}")
    print(f"  - Error Plots: {PLOTS_DIR}")
    
    return {
        'segmented_dir': SEGMENTED_DIR,
        'features_csv': features_path,
        'plots_dir': plots_path
    }