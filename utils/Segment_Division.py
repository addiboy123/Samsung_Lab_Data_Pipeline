import pandas as pd
import os

class CSVSegmenter:
    def __init__(self, base_dir, output_dir, extensions, ratios):
        self.base_dir = base_dir
        self.output_dir = output_dir
        self.extensions = extensions
        self.ratios = ratios

    def split_csv(self, file_path, current_output_dir):
        """Splits a single CSV into multiple files based on provided ratios."""
        if len(self.ratios) != len(self.extensions):
            raise ValueError(f"Ratios ({len(self.ratios)}) and Extensions ({len(self.extensions)}) must match.")

        try:
            df = pd.read_csv(file_path)
            total_rows = len(df)
            if total_rows == 0:
                return

            # Calculate slice sizes
            total_weight = sum(self.ratios)
            target_sizes = [(total_rows * r) // total_weight for r in self.ratios]
            target_sizes[-1] = total_rows - sum(target_sizes[:-1]) # Adjust for rounding

            # Slice and Save
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            start = 0
            for size, ext in zip(target_sizes, self.extensions):
                end = start + size
                chunk = df[start:end]
                
                output_file = os.path.join(current_output_dir, f"{base_name}_{ext}.csv")
                chunk.to_csv(output_file, index=False)
                start = end
                
        except Exception as e:
            print(f"Error splitting {file_path}: {e}")

    def run_segmentation(self):
        """Walks through the base directory and segments every CSV found."""
        for root, _, files in os.walk(self.base_dir):
            for filename in files:
                if filename.endswith(".csv"):
                    file_path = os.path.join(root, filename)
                    
                    # Maintain subfolder structure (e.g., TARIS01)
                    rel_path = os.path.relpath(root, self.base_dir)
                    target_subdir = os.path.join(self.output_dir, rel_path) if rel_path != "." else self.output_dir
                    
                    os.makedirs(target_subdir, exist_ok=True)
                    self.split_csv(file_path, target_subdir)