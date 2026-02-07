import os
import shutil
import pandas as pd
from datetime import datetime

class ETLInitializer:
    def __init__(self, metadata_path, unprocessed_base_dir, raw_output_dir):
        self.metadata_path = metadata_path
        self.unprocessed_base_dir = unprocessed_base_dir
        self.raw_output_dir = raw_output_dir
        
        # Load and clean headers immediately
        df = pd.read_csv(metadata_path)
        df.columns = df.columns.str.strip() # Remove any hidden spaces
        self._metadata_df = df

    def get_group_mapping(self):
        """Dynamically maps TARIS<ID> to its Group."""
        mapping = {}
        for _, row in self._metadata_df.iterrows():
            group = str(row['Group']).strip()
            # User requirement: rename as 'Taris<(Participant ID)>'
            part_id = f"TARIS{str(row['Participant ID']).strip().zfill(2)}"
            
            if group not in mapping:
                mapping[group] = []
            if part_id not in mapping[group]:
                mapping[group].append(part_id)
        return mapping

    def prepare_raw_data(self, start_date_str, end_date_str):
        """Locates Empatica folders and renames them to TARIS<ID>."""
        df = self._metadata_df.copy()
        
        # FIX: Explicitly handle the DD.MM.YYYY format found in your CSV
        df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y', errors='coerce').dt.date
        
        # Filter range
        start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        
        active_participants = df[(df['Date'] >= start) & (df['Date'] <= end)].copy()

        if active_participants.empty:
            print(f"No records found between {start_date_str} and {end_date_str}")
            return []

        processed_dates = []

        for _, row in active_participants.iterrows():
            # ID from first column, e.g., '1' -> '01'
            part_suffix = str(row['Participant ID']).strip().zfill(2)
            # ID from hardware column, e.g., 'TARIS05'
            emp_id = str(row['Empatica ID']).strip()
            # Date in YYYY-MM-DD for folder structure
            date_folder = row['Date'].strftime("%Y-%m-%d")
            
            source_dir = os.path.join(self.unprocessed_base_dir, date_folder)
            
            if not os.path.exists(source_dir):
                continue

            # Find the folder starting with Empatica ID (e.g., 'TARIS05')
            try:
                found_folders = [f for f in os.listdir(source_dir) 
                                if f.startswith(emp_id) and os.path.isdir(os.path.join(source_dir, f))]
                
                for folder in found_folders:
                    dest_name = f"TARIS{part_suffix}"
                    dest_path = os.path.join(self.raw_output_dir, date_folder, dest_name)
                    
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                    
                    if os.path.exists(dest_path):
                        shutil.rmtree(dest_path)
                    
                    shutil.copytree(os.path.join(source_dir, folder), dest_path)
                    print(f"✅ Initialized: {folder} -> {date_folder}/{dest_name}")
                    processed_dates.append(date_folder)
            except Exception as e:
                print(f"❌ Error moving {emp_id}: {e}")

        return list(set(processed_dates))