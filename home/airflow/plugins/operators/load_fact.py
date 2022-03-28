from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    LoadFactOperator for load data to Fact table
    
    INPUT:
    - table : Table name to load
    - redshift_conn_id : redshift connection variable name
    - sql_load_stmt : SQL query for load to the table
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_load_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_load_stmt=sql_load_stmt


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        redshift.run(self.sql_load_stmt)

        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        self.log.info(f"Table {self.table} has been loaded {records[0][0]} records")
