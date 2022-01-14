from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ='',
                 tables='',
                 check_queries='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_queries = check_queries
        self.tables=tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Data quality check has started...")
        self.log.info("Checking to see if tables have data...")
        for table in self.tables:
            records_count = redshift_hook.get_records("SELECT count(*) FROM {}".format(table))
            if len(records_count) < 1 or len(records_count[0]) < 1:
                raise ValueError('Selecting from {} returned nothing. Data quality check failed.'.format(table))
            num_of_rows = records_count[0][0]
            if num_of_rows < 1:
                raise ValueError('{} has no rows. Data quality check failed.'.format(table))
                
        self.log.info("Checking NULL entries...")
        errors_found = 0
        for check_stmt in self.check_queries:
            
            query = check_stmt.get('sql_stmt')
            results = redshift_hook.get_records(query)
            if len(results) > 0:
                errors_found += 1
            
        if(errors_found > 0):
            raise ValueError("{} errors found. Data quality check failed.".format(errors_found))

            