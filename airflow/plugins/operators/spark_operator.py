from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import subprocess
import logging

class SparkOperator(BaseOperator):
    """
    Custom operator to submit Spark jobs
    """
    
    template_fields = ('application', 'application_args')
    
    @apply_defaults
    def __init__(
        self,
        application,
        master='yarn',
        deploy_mode='client',
        application_args=None,
        conf=None,
        *args,
        **kwargs
    ):
        super(SparkOperator, self).__init__(*args, **kwargs)
        self.application = application
        self.master = master
        self.deploy_mode = deploy_mode
        self.application_args = application_args or []
        self.conf = conf or {}
    
    def execute(self, context):
        """Execute Spark job"""
        logging.info(f"Submitting Spark job: {self.application}")
        
        try:
            # Build command
            cmd = [
                'docker-compose', 'exec', 'spark-master',
                'spark-submit',
                '--master', self.master,
                '--deploy-mode', self.deploy_mode
            ]
            
            # Add configuration
            for key, value in self.conf.items():
                cmd.extend(['--conf', f"{key}={value}"])
            
            # Add application
            cmd.append(self.application)
            
            # Add application arguments
            cmd.extend(self.application_args)
            
            logging.info(f"Executing command: {' '.join(cmd)}")
            
            # Execute command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logging.info(f"Spark job completed successfully: {result.stdout}")
            return result.stdout
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Spark job failed: {e.stderr}"
            logging.error(error_msg)
            raise AirflowException(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error executing Spark job: {str(e)}"
            logging.error(error_msg)
            raise AirflowException(error_msg)



