import json
import time
import random
import sys
from kafka import KafkaProducer

def start_producer():
    """
    Initializes the Kafka producer and starts an event loop to stream simulated 
    DNS traffic, generating both legitimate and anomalous (DNScat2) payloads.
    """
    print("Connecting to Kafka cluster...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Successfully connected to Kafka!")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    print("Network Traffic Simulation active.")
    counter = 0
    
    try:
        while True:
            counter += 1
            
            # Architecture Rule: Stochastic distribution to mock user vs attack behavior
            is_attack = random.random() < 0.20
            
            if not is_attack:
                normal_domains = ["google.com", "microsoft.com", "github.com", "netflix.com", "aws.amazon.com"]
                qname = random.choice(normal_domains)
                delay = random.uniform(0.5, 1.5)
            else:
                # Security Rule: Mocking DNScat2 massive encrypted hex payload strings
                random_payload = "".join(random.choices("abcdef0123456789", k=random.randint(40, 70)))
                qname = f"dnscat.{random_payload}.dns-attack.com"
                delay = 0.02

            dns_data = {
                "timestamp": int(time.time()),
                "subdomain": qname,
                "length": len(qname)
            }
            
            producer.send('dns-traffic', value=dns_data)
            
            traffic_status = "⚠️ ATTACK" if is_attack else "🟢 NORMAL"
            print(f"[{traffic_status} - Event #{counter}] -> {qname} ({len(qname)} chars)")
            
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print("\nSimulation intercepted by user. Shutting down gracefully...")
    finally:
        producer.flush()

if __name__ == "__main__":
    start_producer()