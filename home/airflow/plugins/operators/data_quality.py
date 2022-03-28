from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    
    """
    DataQualityOperator for data quality check of data pipelines
    
    INPUT:
    - tables : List table to check
    - redshift_conn_id : redshift connection variable name
    - sql_check_stmts : List query for check data quality
    - expected_results : List expected results for each table
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables="",
                 redshift_conn_id="",
                 sql_check_stmts="",
                 expected_results="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.sql_check_stmts = sql_check_stmts
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Starting to Data Quality checks :')
        for i in range(len(self.tables)):
            sql = self.sql_check_stmts[i]
            exp_result = self.expected_results[i]
            records_quality = redshift.get_records(sql)[0][0]
            
            if records_quality != exp_result:
                raise ValueError(f"Data quality for '{self.tables[i]}' table check failed on '{sql}'. Expected {exp_result} but got {records_quality}.")
            else:
                self.log.info(f"Data quality for '{self.tables[i]}' table check passed!")
        