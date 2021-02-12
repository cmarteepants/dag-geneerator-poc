from pathlib import Path
import pickle


def load_pickled_dags():
    base_dir = Path('.')
    for path in base_dir.glob(r'**/*'):
        if path.suffix == '.pkl':
            with open(path, 'rb') as f:
                dag = pickle.load(f)
                globals()[dag.dag_id] = dag


load_pickled_dags()

# DAG_DISCOVER_SAFE_MODE compatible: When searching for DAGs, Airflow only considers Python files that contain the
# strings "airflow" and "dag"
AIRFLOW_DUMMY_VARIABLE = "airflow"
DAG_DUMMY_VARIABLE = "dag"
