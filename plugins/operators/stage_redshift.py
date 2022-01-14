from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields=('s3_key',)
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION 'us-west-2'
    JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_format=json_format
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing existing data from the designated table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to {} table".format(self.table))
        modified_key = self.s3_key.format(**context)
        s3_full_path = "s3://{}/{}".format(self.s3_bucket, modified_key)
        
        query = StageToRedshiftOperator.copy_sql.format(self.table, 
                                                        s3_full_path, 
                                                        credentials.access_key, 
                                                        credentials.secret_key,
                                                       self.json_format)
        
        redshift.run(query)






