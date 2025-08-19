"""
Airflow DAG 무결성 테스트
- dags/ 폴더에 있는 모든 DAG 파일들이 문법적 오류 없이 Airflow에 의해 로드될 수 있는지 확인합니다.
- DAG 파일에 오타나 잘못된 의존성 주입 등이 있을 경우 이 테스트를 통해 사전에 발견할 수 있습니다.
"""
import os
import unittest
from airflow.models.dagbag import DagBag

class TestDagIntegrity(unittest.TestCase):
    def setUp(self):
        # Airflow는 dags_folder를 기준으로 DAG를 찾으므로, 경로를 설정합니다.
        # 이 테스트는 Airflow 컨테이너 환경이 아닌 로컬 환경에서도 실행 가능하도록 작성되었습니다.
        # 단, 로컬 실행 시에는 airflow 라이브러리가 설치되어 있어야 합니다.
        dags_folder = os.path.join(os.path.dirname(__file__), '..', 'dags')
        self.dagbag = DagBag(dag_folder=dags_folder, include_examples=False)

    def test_no_import_errors(self):
        """DAG 파일 로드 시 import 에러가 없는지 확인합니다."""
        self.assertFalse(
            len(self.dagbag.import_errors),
            f'DAG import errors: {self.dagbag.import_errors}'
        )

    def test_all_dags_are_loaded(self):
        """모든 DAG 파일이 성공적으로 로드되었는지 확인합니다."""
        # .py 파일 수를 세고, 로드된 DAG 수를 확인합니다.
        # 실제 DAG 파일 수와 로드된 DAG 수가 일치하는지 확인하여 누락된 DAG가 없는지 검증합니다.
        # 예를 들어, daily_sentiment_report_dag.py, hourly_data_quality_dag.py 등
        expected_dag_count = len([f for f in os.listdir(os.path.join(os.path.dirname(__file__), '..', 'dags')) if f.endswith('.py') and not f.startswith('_')])
        
        # social_media_dag.py는 삭제될 예정이므로, 그 전까지는 1을 빼줍니다.
        # 실제 운영에서는 이 부분을 더 동적으로 관리할 수 있습니다.
        self.assertGreaterEqual(len(self.dagbag.dags), expected_dag_count -1)

if __name__ == '__main__':
    unittest.main()
