from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

class LoadDimensionOperator(BaseOperator):
    
    """
    LoadDimensionOperator for load data to Dimension table
    
    INPUT:
    - table : Table name to load
    - redshift_conn_id : redshift connection variable name
    - sql_load_stmt : SQL query for load to the table
    - mode : 'delete' value for delete-load functionality, and 'append' value for append-only functionality
    
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_load_stmt="",
                 mode="delete",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_load_stmt = sql_load_stmt
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        if self.mode == "delete" or self.mode == "append":
            if self.mode == "delete":
                redshift.run("DELETE FROM {}".format(self.table))

            redshift.run(self.sql_load_stmt)
        else:
            self.log.info("The 'mode' parameter is not available!!")

        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        self.log.info(f"Table {self.table} has been loaded {records[0][0]} records")