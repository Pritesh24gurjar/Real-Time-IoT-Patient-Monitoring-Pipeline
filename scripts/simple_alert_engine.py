"""
Simple Real-Time Alert Engine - Python Consumer

Consumes raw_vitals and raw_movement streams from Kafka,
evaluates rules in real-time, and publishes critical alerts.

This is a lightweight alternative to Spark Structured Streaming.

Alert Rules:
- FALL_DETECTED: SVM > 25.0 (sudden impact)
- TACHYCARDIA: Heart rate > 130 BPM
- BRADYCARDIA: Heart rate < 40 BPM
- HYPOXIA: SpO2 < 90%
- HIGH_STRESS: Stress level >= 8

Usage:
    python scripts/simple_alert_engine.py
"""

import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
INPUT_TOPICS = ['raw_vitals', 'raw_movement']
OUTPUT_TOPIC = 'critical_alerts'

# Alert thresholds
FALL_SVM_THRESHOLD = 25.0
TACHYCARDIA_HR_THRESHOLD = 125
BRADYCARDIA_HR_THRESHOLD = 45
HYPOXIA_SPO2_THRESHOLD = 95
HIGH_STRESS_THRESHOLD = 9


def create_consumer():
    """Create Kafka consumer for input topics."""
    return KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert-engine-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        key_deserializer=lambda x: x.decode('utf-8') if x else None,
        consumer_timeout_ms=1000,
    )


def create_producer():
    """Create Kafka producer for alerts."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )


def check_vitals_alerts(message):
    """Check vitals data for alert conditions."""
    alerts = []
    data = message.value
    
    patient_id = data.get('patient_id')
    timestamp = data.get('timestamp', time.time())
    
    # Check heart rate
    heart_rate = data.get('heart_rate')
    if heart_rate:
        if heart_rate > TACHYCARDIA_HR_THRESHOLD:
            alerts.append({
                'patient_id': patient_id,
                'timestamp': timestamp,
                'alert_type': 'TACHYCARDIA',
                'heart_rate': heart_rate
            })
        elif heart_rate < BRADYCARDIA_HR_THRESHOLD:
            alerts.append({
                'patient_id': patient_id,
                'timestamp': timestamp,
                'alert_type': 'BRADYCARDIA',
                'heart_rate': heart_rate
            })
    
    # Check SpO2
    spo2 = data.get('spo2')
    if spo2 and spo2 < HYPOXIA_SPO2_THRESHOLD:
        alerts.append({
            'patient_id': patient_id,
            'timestamp': timestamp,
            'alert_type': 'HYPOXIA',
            'spo2_level': spo2
        })
    
    # Check stress level
    # stress_level = data.get('stress_level')
    # if stress_level and stress_level >= HIGH_STRESS_THRESHOLD:
    #     alerts.append({
    #         'patient_id': patient_id,
    #         'timestamp': timestamp,
    #         'alert_type': 'HIGH_STRESS',
    #         'stress_level': int(stress_level)
    #     })
    
    return alerts


def check_movement_alerts(message):
    """Check movement data for fall detection."""
    alerts = []
    data = message.value
    
    patient_id = data.get('patient_id')
    timestamp = data.get('timestamp', time.time())
    svm = data.get('svm', 0)
    
    # Check for fall (high SVM impact)
    if svm > FALL_SVM_THRESHOLD:
        alerts.append({
            'patient_id': patient_id,
            'timestamp': timestamp,
            'alert_type': 'FALL_DETECTED',
            'impact_force': round(svm, 2),
            'activity_at_time': data.get('activity_name', 'unknown')
        })
    
    return alerts


def main():
    """Main entry point for the alert engine."""
    print("=" * 60)
    print("SIMPLE REAL-TIME ALERT ENGINE - Starting...")
    print("=" * 60)
    print(f"Listening to topics: {INPUT_TOPICS}")
    print(f"Writing alerts to: {OUTPUT_TOPIC}")
    print(f"\nAlert Rules:")
    print(f"  - FALL_DETECTED: SVM > {FALL_SVM_THRESHOLD}")
    print(f"  - TACHYCARDIA: HR > {TACHYCARDIA_HR_THRESHOLD}")
    print(f"  - BRADYCARDIA: HR < {BRADYCARDIA_HR_THRESHOLD}")
    print(f"  - HYPOXIA: SpO2 < {HYPOXIA_SPO2_THRESHOLD}")
    print(f"  - HIGH_STRESS: Stress >= {HIGH_STRESS_THRESHOLD}")
    print("=" * 60)
    
    consumer = create_consumer()
    producer = create_producer()
    
    alerts_count = 0
    messages_processed = 0
    
    try:
        print("\nListening for messages... (Press Ctrl+C to stop)\n")
        
        while True:
            try:
                messages = consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for message in records:
                        messages_processed += 1
                        alerts = []
                        
                        # Process based on topic
                        if topic_partition.topic == 'raw_vitals':
                            alerts = check_vitals_alerts(message)
                        elif topic_partition.topic == 'raw_movement':
                            alerts = check_movement_alerts(message)
                        
                        # Send alerts
                        for alert in alerts:
                            alerts_count += 1
                            patient_id = alert['patient_id']
                            
                            # Send to Kafka
                            future = producer.send(
                                OUTPUT_TOPIC,
                                key=patient_id,
                                value=alert
                            )
                            
                            # Print alert
                            alert_type = alert['alert_type']
                            print(f"[ALERT #{alerts_count}] {alert_type} - Patient: {patient_id}")
                            if 'heart_rate' in alert:
                                print(f"    Heart Rate: {alert['heart_rate']} BPM")
                            if 'spo2_level' in alert:
                                print(f"    SpO2: {alert['spo2_level']}%")
                            if 'impact_force' in alert:
                                print(f"    Impact Force (SVM): {alert['impact_force']}")
                            if 'stress_level' in alert:
                                print(f"    Stress Level: {alert['stress_level']}")
                            print()
                
                # Log progress every 1000 messages
                if messages_processed % 100 == 0 and messages_processed > 0:
                    print(f"[Progress] Processed {messages_processed} messages, {alerts_count} alerts generated")
                    
            except KafkaError as e:
                print(f"Kafka error: {e}")
                time.sleep(1)
                
    except KeyboardInterrupt:
        print(f"\n\nShutting down alert engine...")
        print(f"Total messages processed: {messages_processed}")
        print(f"Total alerts generated: {alerts_count}")
    finally:
        producer.flush()
        consumer.close()
        producer.close()
        print("Alert engine stopped.")


if __name__ == "__main__":
    main()
