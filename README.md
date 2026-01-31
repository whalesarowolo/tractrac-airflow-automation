# TracTrac Airflow Automation

This repo contains the Airflow deployment for TracTrac.

## Structure

- `Dockerfile` – Builds custom Airflow image with providers and DAGs.
- `docker-compose.yaml` – Defines the Airflow service for deployment (Coolify).
- `requirements-airflow.txt` – Extra Python packages & providers.
- `dags/` – Airflow DAG definitions.
- `plugins/` – Custom Airflow plugins (optional).

## Local Dev (optional)

```bash
docker compose up -d
