from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    
    """
    StageToRedshiftOperator for load data to Staging table
    
    INPUT:
    - redshift_conn_id : redshift connection variable name
    - aws_credentials_id : aws credentials name
    - table : Table name to load
    - s3_bucket : s3 bucket name
    - s3_key : s3 key name
    - sql_copy_stmt : SQL query for Bulk load to the table
    - json : Parameter for load json data
    
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sql_copy_stmt="",
                 json='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_copy_stmt= sql_copy_stmt
        self.aws_credentials_id = aws_credentials_id
        self.json = json

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = self.sql_copy_stmt.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)
        
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        self.log.info(f"Table {self.table} has been loaded {records[0][0]} records")
        





