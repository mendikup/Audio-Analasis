# Audio Analysis Pipeline

## 1. General Description
This project processes batches of audio files using independent microservices.  
It performs the following steps:
- Scans files and extracts metadata  
- Stores the binary audio data  
- Indexes metadata for search  
- Transcribes speech to text  
- Detects hostile content in transcribed material  

---

## 2. Project Tree
```
kodecode_final_project
│   README.md
│
├── dal
│   ├── files_dal.py
│   ├── transcription_dal.py
│   ├── mongo_dal.py
│   └── elastic_dal.py
│
├── services
│   ├── retriever_service
│   │   └── src
│   │       ├── main.py
│   │       ├── manager.py
│   │       └── data
│   │           └── files_metadata.json
│   │
│   ├── es_indexer_service
│   │   └── src
│   │       ├── main.py
│   │       └── consumer.py
│   │
│   ├── mongo_writer_service
│   │   └── src
│   │       ├── main.py
│   │       └── manager.py
│   │
│   ├── transcription_service
│   │   └── src
│   │       ├── main.py
│   │       └── transcription.py
│   │
│   └── hostility_detection_service
│       └── src
│           ├── main.py
│           └── detector.py
│
└── shared
    ├── config
    │   └── config.yaml
    │
    ├── connectors
    │   ├── kafka_connector.py
    │   ├── mongo_connector.py
    │   └── elastic_connector.py
    │
    └── utils
        ├── config_loader.py
        └── logger.py
```

---

## 3. Services Table
| Service                   | Function                                             | Technologies                        |
|----------------------------|------------------------------------------------------|-------------------------------------|
| Retriever Service          | Scan audio files and publish metadata to Kafka        | Python, Kafka                       |
| ES Indexer Service         | Index metadata in Elasticsearch                      | Python, Kafka, Elasticsearch        |
| Mongo Writer Service       | Store audio binaries in MongoDB GridFS               | Python, Kafka, MongoDB GridFS       |
| Transcription Service      | Convert audio to text (currently synchronous)        | Python, Speech-to-Text, Kafka       |
| Hostility Detection Service| Analyze transcribed text and flag hostile content    | Python, Kafka, Elasticsearch        |

---

## 4. Pipeline Flow
```
Audio Source
   ↓
Retriever Service
   ↓ Kafka → raw metadata topic
        ├─→ Mongo Writer Service → MongoDB GridFS
        ├─→ ES Indexer Service → Elasticsearch
        └─→ Transcription Service (sync) → Kafka → transcribed content topic
             ↓
          Hostility Detection Service
             ↓ Kafka → results topic + Elasticsearch
```

---

## 5. Technologies
- **Kafka** – asynchronous processing and decoupling of services  
- **MongoDB GridFS** – storage for large audio files without size limits  
- **Elasticsearch** – fast search and indexing of metadata and results  
- **Docker** – containerization for simplified deployment  

---

## 6. Architectural Decisions
- Asynchronous pipeline → prevents transcription (a long task) from blocking flow  
- Mongo Writer & ES Indexer run in parallel → reduces latency  
- Microservice principle → each service has a single responsibility  
- Prioritizes scalability & maintainability over a monolithic design  

---

## 7. Installation and Execution
**Prerequisites:**
- Docker & Docker Compose  
- Python 3.10+  

**Environment variables (examples):**
- `KAFKA_BOOTSTRAP_SERVERS`  
- `MONGO_URI`  
- `ELASTIC_URL`  
- `AUDIO_PATH`  
- (others defined in `config.yaml`)  

**Commands:**
```bash
# Build containers
docker compose build

# Start stack
docker compose up

# Stop stack
docker compose down
```

---

## 8. Configuration Structure
- `config.yaml` → default connection details (Kafka, MongoDB, Elasticsearch, paths)  
- Environment variables → override defaults at runtime  
- Shared `utils/config_loader.py` → loads configuration for all services  

---

## 9. Monitoring and Logs
- Each service logs via the shared logger utility  
- View logs inside Docker:
```bash
docker compose logs SERVICE_NAME
```
- Monitor:
  - Kafka offsets  
  - Elasticsearch / MongoDB dashboards  

---

## 10. Future Development
- Transcription → move from synchronous to async Kafka consumption  
- Hostility detection → migrate heavy analysis to Elasticsearch queries  
- Add Kibana dashboards and observability tools  

---

## Hostility Detection Analysis Method
- Hostile & moderate word lists stored in **base64**  
- **Scoring:**
  - Hostile word = ×2 weight  
  - Moderate word = ×1 weight  
  - Special word pairs = extra weight  
- **Formula:**  
  ```
  danger% = ((hostile*2) + moderate + pairs) / total_words * 100
  ```
- **Threshold:**  
  - Flagged if danger% ≥ 15%  
  - Levels:
    - None  
    - Medium  
    - High  
