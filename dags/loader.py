"""Blueprint DAG loader.

Every `*.dag.yaml` in this folder is discovered and turned into an
Airflow DAG. Operators add or edit YAML — this file never changes.
"""

from blueprint import build_all_dags

build_all_dags()
