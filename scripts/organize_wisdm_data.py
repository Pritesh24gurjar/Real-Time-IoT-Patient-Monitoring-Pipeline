"""
Organize WISDM Dataset into a proper, analysis-ready format.

This script:
1. Consolidates all raw data files into unified CSV files
2. Creates separate files for phone and watch data
3. Adds human-readable activity names
4. Creates a summary statistics file
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Paths
DATA_DIR = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data")
WISDM_DIR = DATA_DIR / "wisdm-dataset"
OUTPUT_DIR = DATA_DIR / "wisdm-dataset-organized"

# Activity mapping
ACTIVITY_MAP = {
    'A': 'walking',
    'B': 'jogging',
    'C': 'stairs',
    'D': 'sitting',
    'E': 'standing',
    'F': 'typing',
    'G': 'teeth',
    'H': 'soup',
    'I': 'chips',
    'J': 'pasta',
    'K': 'drinking',
    'L': 'sandwich',
    'M': 'kicking',
    'O': 'catch',
    'P': 'dribbling',
    'Q': 'writing',
    'R': 'clapping',
    'S': 'folding'
}

def load_raw_data(sensor_dir, sensor_type, device_type):
    """Load all raw data files from a directory."""
    all_data = []
    
    if not sensor_dir.exists():
        return None
    
    for file_path in sensor_dir.glob("data_*_{}.txt".format(device_type)):
        try:
            # Read without header - format: subject-id, activity-label, timestamp, x, y, z
            df = pd.read_csv(file_path, header=None, 
                           names=['subject_id', 'activity_code', 'timestamp', 'x', 'y', 'z'],
                           on_bad_lines='skip')
            df['sensor_type'] = sensor_type
            df['device_type'] = device_type
            df['activity_name'] = df['activity_code'].map(ACTIVITY_MAP)
            df['source_file'] = file_path.name
            all_data.append(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return None

def load_arff_data(arff_dir, sensor_type, device_type):
    """Load and parse ARFF files into a unified DataFrame."""
    all_data = []
    
    if not arff_dir.exists():
        return None
    
    for file_path in arff_dir.glob("data_*_{}.arff".format(device_type)):
        try:
            # Parse ARFF file
            with open(file_path, 'r') as f:
                lines = f.readlines()
            
            # Find @data line
            data_start = None
            attributes = []
            for i, line in enumerate(lines):
                line = line.strip()
                if line.startswith('@attribute'):
                    attr_name = line.split()[1].strip('"')
                    attributes.append(attr_name)
                elif line == '@data':
                    data_start = i + 1
                    break
            
            if data_start is None:
                continue
            
            # Parse data lines
            data_lines = lines[data_start:]
            rows = []
            for line in data_lines:
                if line.strip() and not line.startswith('%'):
                    values = line.strip().split(',')
                    if len(values) == len(attributes):
                        rows.append(values)
            
            if rows:
                df = pd.DataFrame(rows, columns=attributes)
                df['sensor_type'] = sensor_type
                df['device_type'] = device_type
                df['source_file'] = file_path.name
                df['activity_name'] = df['ACTIVITY'].map(ACTIVITY_MAP)
                all_data.append(df)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return None

def main():
    print("=" * 60)
    print("WISDM DATASET ORGANIZATION")
    print("=" * 60)
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "raw").mkdir(exist_ok=True)
    (OUTPUT_DIR / "processed").mkdir(exist_ok=True)
    
    print(f"\nOutput directory: {OUTPUT_DIR}")
    
    # ===== LOAD RAW DATA =====
    print("\n" + "=" * 60)
    print("LOADING RAW SENSOR DATA")
    print("=" * 60)
    
    raw_data_configs = [
        (WISDM_DIR / "raw" / "phone" / "accel", "accelerometer", "phone"),
        (WISDM_DIR / "raw" / "phone" / "gyro", "gyroscope", "phone"),
        (WISDM_DIR / "raw" / "watch" / "accel", "accelerometer", "watch"),
        (WISDM_DIR / "raw" / "watch" / "gyro", "gyroscope", "watch"),
    ]
    
    raw_datasets = {}
    for sensor_dir, sensor_type, device_type in raw_data_configs:
        print(f"\nLoading {device_type} {sensor_type} data...")
        df = load_raw_data(sensor_dir, sensor_type, device_type)
        if df is not None:
            raw_datasets[f"{device_type}_{sensor_type}"] = df
            print(f"  ✓ Loaded {len(df):,} readings from {df['source_file'].nunique()} files")
    
    # ===== LOAD ARFF DATA =====
    print("\n" + "=" * 60)
    print("LOADING PROCESSED ARFF DATA")
    print("=" * 60)
    
    arff_data_configs = [
        (WISDM_DIR / "arff_files" / "phone" / "accel", "accelerometer", "phone"),
        (WISDM_DIR / "arff_files" / "phone" / "gyro", "gyroscope", "phone"),
        (WISDM_DIR / "arff_files" / "watch" / "accel", "accelerometer", "watch"),
        (WISDM_DIR / "arff_files" / "watch" / "gyro", "gyroscope", "watch"),
    ]
    
    arff_datasets = {}
    for arff_dir, sensor_type, device_type in arff_data_configs:
        print(f"\nLoading {device_type} {sensor_type} ARFF data...")
        df = load_arff_data(arff_dir, sensor_type, device_type)
        if df is not None:
            arff_datasets[f"{device_type}_{sensor_type}"] = df
            print(f"  ✓ Loaded {len(df):,} samples from {df['source_file'].nunique()} files")
    
    # ===== SAVE CONSOLIDATED FILES =====
    print("\n" + "=" * 60)
    print("SAVING CONSOLIDATED DATASETS")
    print("=" * 60)
    
    # Save raw data
    for key, df in raw_datasets.items():
        output_file = OUTPUT_DIR / "raw" / f"wisdm_raw_{key}.csv"
        df.to_csv(output_file, index=False)
        print(f"  ✓ Saved {output_file.name} ({len(df):,} rows)")
    
    # Save ARFF/processed data
    for key, df in arff_datasets.items():
        output_file = OUTPUT_DIR / "processed" / f"wisdm_processed_{key}.csv"
        df.to_csv(output_file, index=False)
        print(f"  ✓ Saved {output_file.name} ({len(df):,} rows)")
    
    # ===== CREATE COMBINED FILES =====
    print("\n" + "=" * 60)
    print("CREATING COMBINED DATASETS")
    print("=" * 60)
    
    # Combine all raw data
    if raw_datasets:
        all_raw = pd.concat(raw_datasets.values(), ignore_index=True)
        raw_output = OUTPUT_DIR / "wisdm_all_raw.csv"
        all_raw.to_csv(raw_output, index=False)
        print(f"  ✓ Saved {raw_output.name} ({len(all_raw):,} total readings)")
    
    # Combine all processed data
    if arff_datasets:
        all_processed = pd.concat(arff_datasets.values(), ignore_index=True)
        processed_output = OUTPUT_DIR / "wisdm_all_processed.csv"
        all_processed.to_csv(processed_output, index=False)
        print(f"  ✓ Saved {processed_output.name} ({len(all_processed):,} total samples)")
    
    # ===== GENERATE STATISTICS =====
    print("\n" + "=" * 60)
    print("GENERATING STATISTICS")
    print("=" * 60)
    
    stats = []
    
    for key, df in raw_datasets.items():
        stats.append({
            'dataset': key,
            'type': 'raw',
            'total_readings': len(df),
            'unique_subjects': df['subject_id'].nunique(),
            'unique_activities': df['activity_code'].nunique(),
            'subject_range': f"{df['subject_id'].min()} - {df['subject_id'].max()}",
            'x_mean': df['x'].mean(),
            'y_mean': df['y'].mean(),
            'z_mean': df['z'].mean(),
        })
    
    for key, df in arff_datasets.items():
        stats.append({
            'dataset': key,
            'type': 'processed',
            'total_samples': len(df),
            'unique_subjects': df['class'].nunique() if 'class' in df.columns else 'N/A',
            'unique_activities': df['ACTIVITY'].nunique() if 'ACTIVITY' in df.columns else 'N/A',
        })
    
    stats_df = pd.DataFrame(stats)
    stats_file = OUTPUT_DIR / "dataset_statistics.csv"
    stats_df.to_csv(stats_file, index=False)
    print(f"\n  ✓ Saved {stats_file.name}")
    
    # ===== CREATE README =====
    readme_content = f"""# WISDM Dataset - Organized Version

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview

This directory contains the WISDM (Wireless Sensor Data Mining) dataset organized into a clean, analysis-ready format.
The original dataset contains accelerometer and gyroscope data from 51 participants (subject IDs 1600-1650) performing 18 different activities.

## Directory Structure

```
wisdm-dataset-organized/
├── raw/                          # Raw sensor readings (20 Hz sampling)
│   ├── wisdm_raw_phone_accelerometer.csv
│   ├── wisdm_raw_phone_gyroscope.csv
│   ├── wisdm_raw_watch_accelerometer.csv
│   └── wisdm_raw_watch_gyroscope.csv
├── processed/                    # Aggregated features (10-second windows)
│   ├── wisdm_processed_phone_accelerometer.csv
│   ├── wisdm_processed_phone_gyroscope.csv
│   ├── wisdm_processed_watch_accelerometer.csv
│   └── wisdm_processed_watch_gyroscope.csv
├── wisdm_all_raw.csv            # Combined raw data from all sensors
├── wisdm_all_processed.csv      # Combined processed data from all sensors
├── dataset_statistics.csv       # Summary statistics
└── README.md                    # This file
```

## Activity Codes

| Code | Activity   | Code | Activity   |
|------|-----------|------|-----------|
| A    | walking   | J    | pasta     |
| B    | jogging   | K    | drinking  |
| C    | stairs    | L    | sandwich  |
| D    | sitting   | M    | kicking   |
| E    | standing  | O    | catch     |
| F    | typing    | P    | dribbling |
| G    | teeth     | Q    | writing   |
| H    | soup      | R    | clapping  |
| I    | chips     | S    | folding   |

## Raw Data Columns

- **subject_id**: Participant ID (1600-1650)
- **activity_code**: Single letter activity code (see table above)
- **activity_name**: Full activity name
- **timestamp**: Unix timestamp in nanoseconds
- **x, y, z**: Acceleration/angular velocity values
- **sensor_type**: accelerometer or gyroscope
- **device_type**: phone or watch
- **source_file**: Original data file

## Processed Data Columns (ARFF-derived)

The processed data contains aggregated features computed over 10-second windows (200 samples at 20Hz):

- **ACTIVITY**: Activity code
- **X0-X9, Y0-Y9, Z0-Z9**: Binned distribution (30 features)
- **XAVG, YAVG, ZAVG**: Average sensor value per axis
- **XPEAK, YPEAK, ZPEAK**: Time between peaks per axis
- **XABSOLDEV, YABSOLDEV, ZABSOLDEV**: Average absolute deviation
- **XSTANDDEV, YSTANDDEV, ZSTANDDEV**: Standard deviation
- **XVAR, YVAR, ZVAR**: Variance
- **XMFCC0-12, YMFCC0-12, ZMFCC0-12**: MFCC features (39 features)
- **XYCOS, XZCOS, YZCOS**: Cosine distances
- **XYCOR, XZCOR, YZCOR**: Correlations
- **RESULTANT**: Average resultant acceleration
- **class**: Subject ID
- **sensor_type, device_type, source_file, activity_name**: Metadata

## Data Collection Details

- **Participants**: 51 subjects (IDs 1600-1650)
- **Activities**: 18 distinct activities
- **Duration**: 3 minutes per activity per participant
- **Sampling Rate**: 20 Hz
- **Devices**: Nexus 5, Nexus 5X, Galaxy S6 (phones); LG G Watch (smartwatch)
- **Sensors**: Accelerometer and Gyroscope

## Statistics Summary

{stats_df.to_string(index=False)}

## Usage Notes

1. **Raw data** is suitable for time-series analysis and custom feature extraction
2. **Processed data** is ready for machine learning classification tasks
3. Missing subject 1614 in some ARFF files (original dataset limitation)
4. Use `activity_code` or `ACTIVITY` column to map to activity names using the table above

## Original Source

This dataset was collected for activity recognition research. For more details, see:
- WISDM-dataset-description.pdf in the original wisdm-dataset directory
- Original research papers on WISDM activity recognition

## License

Please refer to the original dataset license for usage restrictions.
"""
    
    readme_file = OUTPUT_DIR / "README.md"
    readme_file.write_text(readme_content)
    print(f"  ✓ Saved {readme_file.name}")
    
    print("\n" + "=" * 60)
    print("ORGANIZATION COMPLETE!")
    print("=" * 60)
    print(f"\nOutput location: {OUTPUT_DIR}")
    print(f"\nTotal files created:")
    for f in OUTPUT_DIR.rglob("*"):
        if f.is_file():
            print(f"  - {f.relative_to(OUTPUT_DIR)}")

if __name__ == "__main__":
    main()
