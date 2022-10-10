from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from staging to destination in Redshift")
        insert_stmt = self.table + '_table_insert'
        
        formatted_sql = LoadDimensionOperator.base_sql.format(
            self.table,
            SqlQueries.insert_stmt
        )
        elf.log.info(formatted_sql)
        redshift_hook.run(formatted_sql)
