from utils.initialize import ETLInitializer
from utils.Avro_to_CSV import AvroProcessor
from utils.Segment_Division import CSVSegmenter
import os

def transform_data():
    # --- Configuration ---
    START_DATE = "2026-01-17"
    END_DATE = "2026-02-06"
    
    # Dynamically get the project root path
    # Defaults to current directory if AIRFLOW_HOME is somehow not set
    project_root = os.getenv('AIRFLOW_HOME', os.getcwd())

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

    print("--- ETL SUCCESSFUL ---")
    return SEGMENTED_DIR