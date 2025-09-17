from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import subprocess
import logging

class StarburstOperator(BaseOperator):
    """
    Custom operator to execute Starburst queries
    """
    
    template_fields = ('sql',)
    template_ext = ('.sql',)
    
    @apply_defaults
    def __init__(
        self,
        sql,
        starburst_conn_id='starburst_default',
        autocommit=True,
        parameters=None,
        *args,
        **kwargs
    ):
        super(StarburstOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.starburst_conn_id = starburst_conn_id
        self.autocommit = autocommit
        self.parameters = parameters or {}
    
    def execute(self, context):
        """Execute Starburst query"""
        logging.info(f"Executing Starburst query: {self.sql}")
        
        try:
            # Build command
            cmd = [
                'docker-compose', 'exec', 'starburst-coordinator',
                'starburst',
                '--server', 'starburst-coordinator:8080',
                '--execute', self.sql
            ]
            
            # Execute command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logging.info(f"Query executed successfully: {result.stdout}")
            return result.stdout
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Starburst query failed: {e.stderr}"
            logging.error(error_msg)
            raise AirflowException(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error executing Starburst query: {str(e)}"
            logging.error(error_msg)
            raise AirflowException(error_msg)
