import unittest
from airflow.models import DagBag

class TestDagSerialization(unittest.TestCase):
    def test_dag_import_errors(self):
        dagbag = DagBag(dag_folder='dags/', include_examples=False)
        self.assertFalse(
            len(dagbag.import_errors),
            f"DAG import errors: {dagbag.import_errors}"
        )
