# Airflow Docker Debugging Cheat Sheet

Here's how I found the logs and a cheat sheet for future debugging.

## 💡 Key Insight
**Scheduler logs ≠ Task logs.** The scheduler only shows high-level scheduling events. Your actual task output (including `print()` statements and logic logs) is stored in separate task log files within the container.

---

## 📂 Log File Structure
Airflow organizes logs hierarchically by DAG, Run, and Task:

```text
/opt/airflow/logs/
├── dag_id=latte_flow_etl/
│   └── run_id=manual__2026-02-02T11:27:23.845597+00:00/
│       ├── task_id=alive/
│       │   └── attempt=1.log          ← Task output here!
│       ├── task_id=bronze_to_parquet/
│       │   └── attempt=1.log
│       └── task_id=process_inventory_and_shipments/
│           └── attempt=1.log          ← YOUR PRINTS ARE HERE!

```

## 🔍 How to View Task Logs

### PowerShell (Windows)

Since PowerShell handles quotes and paths differently, use these commands to peek inside the container:

### 1. List recent DAG runs
```powershell
docker-compose exec airflow-scheduler sh -c "ls -la /opt/airflow/logs/dag_id=latte_flow_etl/"
```
### 2. View the log by copy-pasting that ID into this command:
```powershell
docker-compose exec airflow-scheduler sh -c "cat '/opt/airflow/logs/dag_id=latte_flow_etl/run_id=PASTE_ID_HERE/task_id=process_inventory_and_shipments/attempt=1.log'"
```
### Method B: The Pro Way (Best for active debugging)

```powershell
$LATEST_RUN = docker-compose exec airflow-scheduler ls -t /opt/airflow/logs/dag_id=latte_flow_etl/ | Select-Object -First 1; docker-compose exec airflow-scheduler sh -c "cat '/opt/airflow/logs/dag_id=latte_flow_etl/$LATEST_RUN/task_id=process_inventory_and_shipments/attempt=1.log'"
```

### Linux / Mac

You can use wildcards or direct paths:
```bash
docker-compose exec airflow-scheduler cat /opt/airflow/logs/dag_id=latte_flow_etl/run_id=*/task_id=process_inventory_and_shipments/attempt=1.log
```

### ⚡ Quick Commands Reference

| Goal | Command |
| :--- | :--- |
| **Scheduler logs (live)** | `docker-compose logs -f airflow-scheduler` |
| **Webserver logs** | `docker-compose logs -f airflow-webserver` |
| **All containers logs** | `docker-compose logs -f` |
| **Last 100 lines** | `docker-compose logs --tail=100 airflow-scheduler` |
| **Enter the container** | `docker-compose exec airflow-scheduler bash` |
| **Check container status** | `docker-compose ps` |

🌐 Alternative: Airflow UI

    Go to http://localhost:8080

    Click on your DAG → Click on the specific task instance (e.g., the red square).

    Click the Logs tab. This displays the exact same content as the attempt=1.log file.

🛠️ Debugging Tips

    Task fails silently? → Check the Task log, not the Scheduler log.

    Find latest run_id? → Use ls -lt inside the logs directory to sort by time.

    Retried a task? → Airflow creates new files: check attempt=2.log, attempt=3.log, etc.

    Import Errors? → These are the exception! They appear in the Scheduler log when the DAG is first being parsed.