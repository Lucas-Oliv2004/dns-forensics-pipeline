# Real-Time DNS Forensics Pipeline with Apache Kafka & Apache Spark

This repository implements a high-performance, real-time Big Data pipeline tailored for Cybersecurity Analytics (SIEM). The system ingests live network events via Apache Kafka and utilizes Apache Spark to apply statistical aggregation and detect DNS Tunneling anomalies (such as DNScat2 exfiltration) in milliseconds.

---

## Pipeline Demonstration

### Ecosystem Overview
Below is the complete real-time integration displaying the network probe simulation streaming stochastic payloads on one terminal, while the Spark engine captures and evaluates the incoming micro-batches on the other.
![Pipeline General Demonstration](images/Screenshot%20-%20Demonstration.png)

---

## Technology Stack
* **Data Processing:** Apache Spark (PySpark)
* **Event Streaming:** Apache Kafka & Docker
* **Language:** Python 3.14+
* **Environment:** Windows 11 with Hadoop/Winutils local integration

---

## Cyber Threat Scenario: DNS Tunneling Detection
DNS Tunneling is a sophisticated cyberattack method where an adversary bypasses firewall restrictions by encapsulating non-DNS protocols (such as SSH, HTTP, or data exfiltration strings) within standard DNS queries.

Tools like **DNScat2** leverage this vulnerability by encrypting stolen data and placing it as randomized, extremely long subdomains. While human web traffic queries short, standard domains, an active automated data exfiltration burst generates a massive volume of packets containing abnormally large payload lengths.

This pipeline monitors the moving average of domain lengths (`avg_length`) in real-time. Subdomains with a mathematical average higher than **35 characters** within a micro-batch are instantly flagged as a security anomaly.

---

## Project Architecture

The pipeline is entirely decoupled into functional microservices:

### 1. Network Capture Probe (`producer.py`)
Generates a stochastic distribution of network events: 80% represents legitimate human browsing behavior, while 20% mimics aggressive, low-delay (20ms) C2 server data exfiltration bursts carrying encrypted hexadecimal strings.
![Producer Output Stream](images/Screenshot%20-%20producer.png)

### 2. Analytics Core Engine (`spark_processor.py`)
Consumes raw bytes from Kafka, converts memory buffers into a Spark Distributed Dataframe, performs windowed statistical group-by reductions, and applies the threat assessment signatures based on structural query lengths.
![Spark Ingestion Interface](images/Screenshot%20-%20spark_processor.png)

---

## Getting Started

### 1. Prerequisites
Ensure your environment has Docker, Python, and Java configured. For Windows execution, make sure Hadoop Winutils are located at `C:\hadoop\bin\winutils.exe`.

### 2. Spin up the Kafka Environment
Start your Docker container hosting the Zookeeper and Kafka broker instances:
```bash
docker-compose up -d
```

### 3. Run the Analytics Engine
Open a terminal inside the project directory and boot the PySpark processing stream:
```bash
python spark_processor.py
```

### 4. Ignite the Network Traffic Ingestion
Open a secondary terminal split and run the probe simulator to begin streaming data into the queue:
```bash
python producer.py
```

### 5. Pipeline Monitoring Outputs
When an anomaly threshold is broken, the Spark engine prints structural dataframes directly to the terminal console.
