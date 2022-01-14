from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 query='',
                 table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
      
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.table = table

    def execute(self, context):
        
        self.log.info("Loading data into the dimension table named {}".format(self.table))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.run(self.query)
