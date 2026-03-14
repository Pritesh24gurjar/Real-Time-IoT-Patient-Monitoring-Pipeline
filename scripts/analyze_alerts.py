"""Analyze alert distribution in the dataset."""
import pandas as pd

df = pd.read_csv('data/clean_smartwatch_health_data.csv')

print('=' * 60)
print('ALERT DISTRIBUTION IN DATASET')
print('=' * 60)

# HIGH_STRESS (stress >= 8)
print('\n1. HIGH_STRESS Alerts (Stress Level >= 8):')
high_stress = df[df['Stress Level'] >= 8]
print(f'   Count: {len(high_stress)} ({len(high_stress)/len(df)*100:.1f}%)')
for level in [8, 9, 10]:
    count = len(df[df['Stress Level'] == level])
    print(f'     - Level {level}: {count} records')

# TACHYCARDIA (HR > 130)
print('\n2. TACHYCARDIA Alerts (Heart Rate > 130 BPM):')
tachycardia = df[df['Heart Rate (BPM)'] > 130]
print(f'   Count: {len(tachycardia)} ({len(tachycardia)/len(df)*100:.1f}%)')
print(f'   HR range: {df["Heart Rate (BPM)"].min():.1f} - {df["Heart Rate (BPM)"].max():.1f} BPM')

# BRADYCARDIA (HR < 40)
print('\n3. BRADYCARDIA Alerts (Heart Rate < 40 BPM):')
bradycardia = df[df['Heart Rate (BPM)'] < 40]
print(f'   Count: {len(bradycardia)} ({len(bradycardia)/len(df)*100:.1f}%)')

# HYPOXIA (SpO2 < 90)
print('\n4. HYPOXIA Alerts (SpO2 < 90%):')
hypoxia = df[df['Blood Oxygen Level (%)'] < 90]
print(f'   Count: {len(hypoxia)} ({len(hypoxia)/len(df)*100:.1f}%)')
print(f'   SpO2 range: {df["Blood Oxygen Level (%)"].min():.2f}% - {df["Blood Oxygen Level (%)"].max():.2f}%')

print('\n' + '=' * 60)
print('SUMMARY - Alert Frequency')
print('=' * 60)
alerts = {
    'HIGH_STRESS': len(high_stress),
    'TACHYCARDIA': len(tachycardia),
    'BRADYCARDIA': len(bradycardia),
    'HYPOXIA': len(hypoxia)
}
for alert_type, count in sorted(alerts.items(), key=lambda x: x[1], reverse=True):
    pct = count/len(df)*100
    bar = '█' * int(pct * 2)
    print(f'{alert_type:15} {count:5} ({pct:5.1f}%) {bar}')

print(f'\nTotal records: {len(df):,}')
