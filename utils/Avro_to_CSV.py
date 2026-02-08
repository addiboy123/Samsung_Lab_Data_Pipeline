import os
import re
import shutil
import csv
from avro.datafile import DataFileReader
from avro.io import DatumReader

class AvroProcessor:
    def __init__(self, raw_base_dir, output_csv_dir, organized_dir):
        self.raw_base_dir = raw_base_dir
        self.output_csv_dir = output_csv_dir
        self.organized_dir = organized_dir

    def rename_avro_files(self, date_folder):
        """Renames files within a specific date subfolder."""
        path = os.path.join(self.raw_base_dir, date_folder)
        if not os.path.exists(path):
            return
            
        folders = [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
        for folder_name in folders:
            raw_data_path = os.path.join(path, folder_name, "raw_data", "v6")
            if not os.path.exists(raw_data_path):
                continue

            avro_files = [f for f in os.listdir(raw_data_path) if f.endswith('.avro')]
            for i, avro_file in enumerate(avro_files, start=1):
                new_name = f"{folder_name}_{i}.avro" if len(avro_files) > 1 else f"{folder_name}.avro"
                os.rename(os.path.join(raw_data_path, avro_file), os.path.join(raw_data_path, new_name))

    def process_avro_to_csv(self):
        """Walks through raw data and extracts EDA/BVP signals to CSV."""
        os.makedirs(self.output_csv_dir, exist_ok=True)
        
        for root, _, files in os.walk(self.raw_base_dir):
            for file in files:
                if file.endswith(".avro"):
                    self._convert_and_append_file(os.path.join(root, file))

    def _convert_and_append_file(self, file_path):
        """Extracts data and appends it to a single participant CSV."""
        try:
            with open(file_path, "rb") as f:
                reader = DataFileReader(f, DatumReader())
                data = next(reader)
                
                # Use regex to get the ID (e.g., TARIS12) from 'TARIS12_1.avro' or 'TARIS12.avro'
                base_file_name = os.path.basename(file_path)
                match = re.search(r'TARIS\d+', base_file_name)
                participant_id = match.group() if match else base_file_name.split('_')[0].split('.')[0]

                for signal in ["eda", "bvp"]:
                    sig_data = data["rawData"][signal]
                    timestamps = [
                        round(sig_data["timestampStart"] + i * (1e6 / sig_data["samplingFrequency"]))
                        for i in range(len(sig_data["values"]))
                    ]
                    
                    # File naming: e.g., eda_TARIS12.csv (regardless of which chunk this is)
                    out_file = os.path.join(self.output_csv_dir, f"{signal}_{participant_id}.csv")
                    
                    # 'a' mode appends. If it's the first time, write the header.
                    file_exists = os.path.isfile(out_file)
                    with open(out_file, 'a', newline='') as f_out:
                        writer = csv.writer(f_out)
                        if not file_exists:
                            writer.writerow(["unix_timestamp", signal])
                        writer.writerows(zip(timestamps, sig_data["values"]))
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    def organize_by_subject(self, mapping):
        """Moves CSVs into TARIS folders and then into Activity groups."""
        # Step 1: Segregate by TARIS ID
        for file_name in os.listdir(self.output_csv_dir):
            match = re.search(r'TARIS\d+', file_name)
            if match:
                taris_id = match.group()
                dest_dir = os.path.join(self.organized_dir, taris_id)
                os.makedirs(dest_dir, exist_ok=True)
                shutil.move(os.path.join(self.output_csv_dir, file_name), os.path.join(dest_dir, file_name))

        # Step 2: Group by Activity
        for group, subjects in mapping.items():
            group_path = os.path.join(self.organized_dir, group)
            os.makedirs(group_path, exist_ok=True)
            for subject in subjects:
                subject_current_path = os.path.join(self.organized_dir, subject)
                if os.path.exists(subject_current_path):
                    shutil.move(subject_current_path, group_path)