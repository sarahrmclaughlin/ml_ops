### All things ML Ops

##### Two Projects in this Repo: ML Ops Watermarking and EvidentlyAI for drift check

##### **ML Ops Watermarking project**
- Using watermarking to monitor upstream changes that would affect model
- This is a demonstration of watermarking in stream data processing (similar to Flink/Spark), where events are classified as "on-time" or "late" based on a watermark threshold.

#### Breakdown of the Steps:
- Start Airflow: Ensure Docker is running and execute docker compose up -d (already done).

- **Trigger DAG in "normal" mode:**

- Open http://localhost:8080 in your browser.
- Navigate to the DAGs page, find watermark_demo, and click "Trigger DAG".
- In the configuration JSON, set: {"run_mode": "normal"}.
- Result: Generates and processes 10 events, all classified as ON-TIME (no late events, so none are quarantined).
- Check output in processed and watermark state in data/watermarks/watermark_state.json.


- **Trigger DAG in "inject_late" mode: Trigger the same DAG again, but with: {"run_mode": "inject_late"}.**
- Result: Generates 3 on-time events + 1 deliberately late event (with feature1=10000), which gets quarantined (handled separately as late data).
- This shows how watermarking detects and isolates late-arriving data that could skew ML model predictions.

#### *CLI*                                                                                                                      
  ```
  - Turn on Docker Desktop and make sure it is running and active
  1. docker compose up -d                
  2. Go to localhost:8080 → trigger watermark_demo with {"run_mode": "normal"} → 10 ON-TIME events                                         
  3. Trigger again with {"run_mode": "inject_late"} → 3 on-time + 1 quarantined feature1=10000    
  4. Check data/processed/ for output files and data/watermarks/watermark_state.json for persisted state ```

#### Key Concept (Watermarking):
- Watermark = Max event timestamp seen so far - 30 seconds (delay).
- Events with event_time >= watermark → Processed as on-time.
- Events with event_time < watermark → Marked as late and quarantined (not used - for real-time processing).
- This prevents issues like late data affecting model accuracy in streaming ML pipelines.                                        


 ┌─────────────────────────────────┬──────────────────────────────────────────────────────────┐
  │              File               │                          Status                          │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ docker-compose.yaml             │ Added ./data and ./src volume mounts                     │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ src/watermark_manager.py        │ Watermark state tracking with JSON persistence           │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ src/event_generator.py          │ Generates ndjson event batches (normal + late injection) │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ src/stream_processor.py         │ Routes events ON-TIME vs LATE, logs formatted output     │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ dags/watermark_demo_dag.py      │ 3-task Airflow DAG wiring it all together                │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ tests/test_watermark_manager.py │ 16 tests covering boundary, persistence, state           │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤
  │ tests/test_event_generator.py   │ 13 tests covering batches, late events, file output      │
  ├─────────────────────────────────┼──────────────────────────────────────────────────────────┤                                           
  │ tests/test_stream_processor.py  │ 10 tests including full end-to-end two-run scenario      │
  └─────────────────────────────────┴──────────────────────────────────────────────────────────┘   


