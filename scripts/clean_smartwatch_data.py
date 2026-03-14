"""
Clean the unclean smartwatch health data.

Issues identified in the raw data:
1. Missing values (empty cells) in all columns
2. Inconsistent Activity Level labels (typos like "Actve", "Seddentary", "Highly_Active" vs "Highly Active")
3. Invalid values: "ERROR" in Sleep Duration, "nan" in Activity Level
4. Invalid Stress Level values: "Very High" (non-numeric)
5. Outliers in Heart Rate (e.g., 247 BPM, values < 40)
6. Outliers in Step Count (e.g., 33956, 30266)
7. Invalid Sleep Duration values (negative or unrealistic)
8. Blood Oxygen > 100% (invalid)
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Paths
DATA_DIR = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data")
INPUT_FILE = DATA_DIR / "unclean_smartwatch_health_data.csv"
OUTPUT_FILE = DATA_DIR / "clean_smartwatch_health_data.csv"

# Load data
print(f"Loading data from {INPUT_FILE}...")
df = pd.read_csv(INPUT_FILE)

print(f"\n=== INITIAL DATA ANALYSIS ===")
print(f"Shape: {df.shape}")
print(f"\nColumn types:\n{df.dtypes}")
print(f"\nMissing values:\n{df.isnull().sum()}")
print(f"\nSample rows:\n{df.head()}")

# Store original count
original_count = len(df)
print(f"\nOriginal row count: {original_count}")

# === CLEANING STEPS ===

print(f"\n=== CLEANING PROCESS ===")

# 1. Drop rows where User ID is missing (can't identify the user)
before = len(df)
df = df.dropna(subset=['User ID'])
print(f"1. Dropped {before - len(df)} rows with missing User ID")

# 2. Standardize Activity Level labels
activity_mapping = {
    'Actve': 'Active',
    'Seddentary': 'Sedentary',
    'Highly_Active': 'Highly Active',
    'nan': np.nan,
}
df['Activity Level'] = df['Activity Level'].replace(activity_mapping)
df['Activity Level'] = df['Activity Level'].str.strip() if df['Activity Level'].dtype == 'object' else df['Activity Level']
print("2. Standardized Activity Level labels")

# 3. Convert Stress Level to numeric, coercing errors to NaN
df['Stress Level'] = pd.to_numeric(df['Stress Level'], errors='coerce')
print("3. Converted Stress Level to numeric (invalid values -> NaN)")

# 4. Replace 'ERROR' in Sleep Duration with NaN
df['Sleep Duration (hours)'] = df['Sleep Duration (hours)'].replace('ERROR', np.nan)
df['Sleep Duration (hours)'] = pd.to_numeric(df['Sleep Duration (hours)'], errors='coerce')
print("4. Replaced 'ERROR' in Sleep Duration with NaN")

# 5. Remove rows with invalid Heart Rate (normal range: 40-200 BPM for adults during various activities)
before = len(df)
df = df[(df['Heart Rate (BPM)'] >= 40) & (df['Heart Rate (BPM)'] <= 200)]
print(f"5. Removed {before - len(df)} rows with invalid Heart Rate (outside 40-200 BPM)")

# 6. Remove rows with invalid Blood Oxygen (normal range: 90-100%)
before = len(df)
df = df[(df['Blood Oxygen Level (%)'] >= 90) & (df['Blood Oxygen Level (%)'] <= 100)]
print(f"6. Removed {before - len(df)} rows with invalid Blood Oxygen (outside 90-100%)")

# 7. Remove rows with invalid Step Count (0-25000 reasonable range)
before = len(df)
df = df[(df['Step Count'] >= 0) & (df['Step Count'] <= 25000)]
print(f"7. Removed {before - len(df)} rows with invalid Step Count (outside 0-25000)")

# 8. Remove rows with invalid Sleep Duration (0-12 hours reasonable range)
before = len(df)
df = df[(df['Sleep Duration (hours)'] >= 0) & (df['Sleep Duration (hours)'] <= 12)]
print(f"8. Removed {before - len(df)} rows with invalid Sleep Duration (outside 0-12 hours)")

# 9. Remove rows with invalid Stress Level (0-10 scale)
before = len(df)
df = df[(df['Stress Level'] >= 0) & (df['Stress Level'] <= 10)]
print(f"9. Removed {before - len(df)} rows with invalid Stress Level (outside 0-10)")

# 10. Drop rows with missing Activity Level (categorical, can't impute easily)
before = len(df)
df = df.dropna(subset=['Activity Level'])
print(f"10. Dropped {before - len(df)} rows with missing Activity Level")

# 11. Impute missing numeric values with median (more robust than mean)
numeric_cols = ['Heart Rate (BPM)', 'Blood Oxygen Level (%)', 'Step Count', 
                'Sleep Duration (hours)', 'Stress Level']
for col in numeric_cols:
    median_val = df[col].median()
    missing_count = df[col].isnull().sum()
    df[col] = df[col].fillna(median_val)
    if missing_count > 0:
        print(f"11. Imputed {missing_count} missing values in '{col}' with median ({median_val:.2f})")

# Round numeric columns for cleaner output
df['User ID'] = df['User ID'].astype(int)
df['Heart Rate (BPM)'] = df['Heart Rate (BPM)'].round(2)
df['Blood Oxygen Level (%)'] = df['Blood Oxygen Level (%)'].round(2)
df['Step Count'] = df['Step Count'].round(0).astype(int)
df['Sleep Duration (hours)'] = df['Sleep Duration (hours)'].round(2)
df['Stress Level'] = df['Stress Level'].astype(int)

# Reset index
df = df.reset_index(drop=True)

print(f"\n=== CLEANED DATA SUMMARY ===")
print(f"Final row count: {len(df)}")
print(f"Rows removed: {original_count - len(df)} ({(original_count - len(df))/original_count*100:.1f}%)")
print(f"\nFinal shape: {df.shape}")
print(f"\nColumn types:\n{df.dtypes}")
print(f"\nMissing values:\n{df.isnull().sum()}")
print(f"\nStatistics:\n{df.describe()}")
print(f"\nActivity Level distribution:\n{df['Activity Level'].value_counts()}")
print(f"\nStress Level distribution:\n{df['Stress Level'].value_counts().sort_index()}")

# Save cleaned data
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n=== CLEANED DATA SAVED TO: {OUTPUT_FILE} ===")
