"""
Cloud Composer / Airflow secure patterns reference
-------------------------------------------------
This file answers:
1) Security, secrets, IAM on GCP
2) RBAC and team isolation in shared Composer
3) Variables/connections without hard-coding secrets
4) CI/CD, deployment, versioning for DAGs
5) Preventing one DAG change from breaking another

This is a reference file, not one deployable DAG as-is.
Copy the relevant sections into your repo.
"""

from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.secret_manager import SecretManagerHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ============================================================================
# 1) SECURITY, SECRETS, IAM ON GCP
# ============================================================================
# Recommended production pattern for Cloud Composer:
# - Store secrets in Google Secret Manager
# - Configure Composer secrets backend once at environment level
# - Use the Composer environment service account with least privilege
# - Grant only roles/secretmanager.secretAccessor on required secrets
# - Avoid putting passwords, tokens, or full connection URIs in DAG code
#
# Important architecture notes:
# - Airflow Variables for non-sensitive config only when possible
# - Sensitive values -> Secret Manager
# - Airflow Connections -> resolve from Secret Manager backend automatically
# - Teams should not get broad write access to the Composer bucket


def print_runtime_config():
    """Example: non-secret config from Variables/ENV, not from hard-coded values."""
    dataset = Variable.get("bq_dataset", default_var="default_dataset")
    team = Variable.get("dag_owner_team", default_var="platform")
    print({"dataset": dataset, "team": team})


with DAG(
    dag_id="secure_variable_example",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["security", "reference"],
    default_args={
        "owner": "team-data-eng",
        "retries": 1,
    },
    doc_md="""
    Example DAG showing how to read non-sensitive runtime config using Airflow Variables.
    In Composer, configure Secret Manager backend so Variable.get() can resolve secrets
    from Secret Manager when needed.
    """,
) as secure_variable_example:
    PythonOperator(
        task_id="print_runtime_config",
        python_callable=print_runtime_config,
    )


# ============================================================================
# 2) SENSITIVE VALUES: SECRET MANAGER DIRECT ACCESS (WHEN REALLY NEEDED)
# ============================================================================
# Prefer hooks/operators using conn_id over manual secret fetching.
# But if a custom API token is needed, fetch it at task runtime.


def call_external_api():
    hook = SecretManagerHook()
    secret_value = hook.access_secret(
        secret_id="third-party-api-token",
        project_id="YOUR_GCP_PROJECT_ID",
        version_id="latest",
    )
    token = secret_value.payload.data.decode("UTF-8")

    # Use token only in-memory. Never log it.
    masked = token[:3] + "***"
    print({"api_token_preview": masked, "message": "Token fetched securely at runtime"})


with DAG(
    dag_id="secret_manager_direct_runtime_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["security", "secret-manager"],
    default_args={"owner": "team-analytics"},
) as secret_manager_direct_runtime_example:
    PythonOperator(
        task_id="call_external_api",
        python_callable=call_external_api,
    )


# ============================================================================
# 3) CONNECTIONS WITHOUT HARD-CODING VALUES
# ============================================================================
# Best practice:
# - Define a connection ID in DAG code, such as "bq_default", "postgres_sales"
# - Store the actual connection URI in Secret Manager with naming convention:
#     airflow-connections-<conn_id>
# - Airflow resolves it automatically via configured secrets backend

SQL = "SELECT CURRENT_TIMESTAMP() AS ts"

with DAG(
    dag_id="connection_backend_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["connections", "reference"],
    default_args={"owner": "team-finance"},
) as connection_backend_example:
    BigQueryInsertJobOperator(
        task_id="run_query",
        gcp_conn_id="google_cloud_default",  # conn id only, no secret in code
        configuration={
            "query": {
                "query": SQL,
                "useLegacySql": False,
            }
        },
    )


# ============================================================================
# 4) TEAM ISOLATION / DAG OWNERSHIP IN SHARED COMPOSER
# ============================================================================
# Cloud Composer IAM controls environment-level access.
# For multiple teams in one Composer environment, use these controls together:
#
# A. Git repo structure
#    dags/
#      team_a/
#      team_b/
#      shared_lib/
#
# B. CODEOWNERS + required reviewers per team path
# C. Separate deployment prefixes or branch protection checks
# D. Airflow UI roles / DAG-level access policies where supported
# E. Naming + tagging standard:
#      dag_id="team_a__daily_orders_load"
#      owner="team-a"
#      tags=["team-a", "domain-orders", "prod"]
# F. Only platform team can modify shared_lib/
# G. Teams do not get direct write access to Composer bucket in production
#
# NOTE:
# In practice, strong team isolation is easier with separate Composer environments
# per domain or criticality boundary (for example: shared-dev, team-prod-a, team-prod-b).

TEAM = "team_a"

with DAG(
    dag_id=f"{TEAM}__sample_owned_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=[TEAM, "owned", "prod"],
    default_args={
        "owner": TEAM,
        "email": ["team_a_oncall@example.com"],
    },
) as team_owned_dag:
    BashOperator(
        task_id="team_a_task",
        bash_command="echo 'owned by team_a'",
    )


# ============================================================================
# 5) SAFE DAG DESIGN TO PREVENT CROSS-DAG BREAKAGE
# ============================================================================
# Problem: a common utility change broke another DAG.
# Prevention:
# - Put reusable code in versioned shared modules
# - Minimize import-time side effects
# - Avoid global heavy logic in DAG files
# - Test every DAG import in CI
# - Separate shared libraries from team DAG code
# - Promote changes through dev -> staging -> prod Composer
# - Use canary deployment for shared modules


def build_dag(team: str, dag_suffix: str, schedule: str | None):
    with DAG(
        dag_id=f"{team}__{dag_suffix}",
        start_date=datetime(2025, 1, 1),
        schedule=schedule,
        catchup=False,
        tags=[team, "factory-pattern"],
        default_args={"owner": team},
    ) as dag:
        BashOperator(task_id="start", bash_command="echo start")
        BashOperator(task_id="end", bash_command="echo end")
    return dag


team_a_factory_dag = build_dag("team_a", "factory_example", "@daily")
team_b_factory_dag = build_dag("team_b", "factory_example", "@hourly")


# ============================================================================
# 6) CI/CD DESIGN FOR CLOUD COMPOSER
# ============================================================================
# Recommended pipeline:
# 1. Developer creates feature branch
# 2. Pull request opened
# 3. CI runs:
#    - formatting / linting
#    - import tests for all DAGs
#    - unit tests for shared utils/operators
#    - policy checks (naming, owner, tags, forbidden secrets in code)
# 4. Mandatory code review via CODEOWNERS
# 5. Merge to main
# 6. Deploy to dev/staging Composer
# 7. Smoke tests / trigger selected DAGs
# 8. Manual approval or automated gate
# 9. Deploy to prod Composer
#
# Deployment methods commonly used:
# - gsutil rsync to Composer DAG bucket
# - gcloud composer storage dags import (where applicable)
# - Git-sync based flow depending on environment setup

CLOUDBUILD_TEST = r"""
# cloudbuild-test.yaml
steps:
- name: 'python:3.11-slim'
  entrypoint: 'bash'
  args:
  - '-c'
  - |
      pip install -r requirements.txt
      pip install pytest flake8
      flake8 dags tests
      pytest tests -v
"""

CLOUDBUILD_DEPLOY = r"""
# cloudbuild-deploy.yaml
steps:
- name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
  entrypoint: 'bash'
  args:
  - '-c'
  - |
      DAG_BUCKET=$(gcloud composer environments describe PROD_COMPOSER \
        --location us-central1 \
        --format='value(config.dagGcsPrefix)')
      gsutil -m rsync -r dags/ ${DAG_BUCKET}/
"""


# ============================================================================
# 7) CI TEST EXAMPLES
# ============================================================================
TEST_DAG_IMPORTS = r"""
# tests/test_dag_imports.py
from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert dag_bag.import_errors == {}
"""

TEST_DAG_CONVENTIONS = r"""
# tests/test_dag_conventions.py
from airflow.models import DagBag


def test_all_dags_have_owner_and_tags():
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert dag.owner not in (None, '', 'airflow')
        assert dag.tags
        assert '__' in dag_id  # example convention: team__dag_name
"""

PRECOMMIT_HINTS = r"""
# Example local quality commands
python -m pip install -r requirements.txt
python -m pip install pytest flake8 black isort
black dags/ tests/
isort dags/ tests/
flake8 dags/ tests/
pytest tests/ -v
"""


# ============================================================================
# 8) COMMANDS TO UNDERSTAND / USE IN REAL PROJECTS
# ============================================================================
COMMANDS = r"""
# Enable Secret Manager API
gcloud services enable secretmanager.googleapis.com

# Grant Composer environment service account access to a specific secret
gcloud secrets add-iam-policy-binding third-party-api-token \
  --member="serviceAccount:COMPOSER_ENV_SA@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Configure Airflow secrets backend in Composer
# (Use the right environment name, region, and project id)
gcloud composer environments update COMPOSER_ENV \
  --location us-central1 \
  --update-airflow-configs=secrets-backend=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend,secrets-backend_kwargs='{\"project_id\":\"PROJECT_ID\",\"connections_prefix\":\"airflow-connections\",\"variables_prefix\":\"airflow-variables\"}'

# Create an Airflow connection secret in Secret Manager
echo -n 'postgresql://user:ENCODED_PASSWORD@host:5432/dbname' | \
  gcloud secrets create airflow-connections-postgres_sales --data-file=-

# Create an Airflow variable secret in Secret Manager
echo -n 'analytics_dataset' | \
  gcloud secrets create airflow-variables-bq_dataset --data-file=-

# Get Composer DAG bucket
gcloud composer environments describe COMPOSER_ENV \
  --location us-central1 \
  --format='value(config.dagGcsPrefix)'

# Deploy dags manually with rsync
gsutil -m rsync -r dags/ gs://YOUR_COMPOSER_DAG_BUCKET/

# Run an Airflow CLI command in Composer
gcloud composer environments run COMPOSER_ENV \
  --location us-central1 dags list

# Trigger a DAG for smoke test
gcloud composer environments run COMPOSER_ENV \
  --location us-central1 dags trigger -- team_a__sample_owned_dag
"""


# ============================================================================
# 9) PRACTICAL INTERVIEW ANSWERS (SHORT FORM)
# ============================================================================
INTERVIEW_NOTES = r"""
Q: How do you store DB credentials/API keys for Airflow DAGs in Composer?
A: Use Secret Manager as Airflow secrets backend, grant secretAccessor only to the Composer environment service account, and reference conn_id / Variable.get() instead of hard-coding values.

Q: How do you handle multiple teams in one Composer environment?
A: Use least-privilege IAM, Airflow UI RBAC where supported, repo folder isolation, CODEOWNERS, per-team naming/tagging/owner standards, protected shared libraries, and ideally separate environments for strong isolation.

Q: How do you manage Airflow variables and connections in production?
A: Keep connection IDs and variable names in code, actual secret values in Secret Manager, non-sensitive config in Variables/config files, and provision them through Terraform or CI/CD.

Q: CI/CD for DAGs?
A: PR-based workflow with lint, DAG import tests, unit tests, code review, deploy to lower environment first, run smoke/integration tests, then promote to production.

Q: How to stop one DAG change from breaking another?
A: Separate shared code, version utilities, add DAG parse tests, integration tests, staging Composer, canary rollout, and stronger ownership boundaries.
"""


if __name__ == "__main__":
    # This file is primarily for reading. Printing brief anchors only.
    print("Open this file and review sections 1-9.")
