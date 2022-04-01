from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator


class LoadFactOperator(BaseOperator):
    # background color of the operator in UI
    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id: str,
                 table_name: str,
                 sql_query: str,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query

    def execute(self, context):

        print('\nGetting redshift connection details ...')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        COPY_SQL = f"""
        INSERT INTO {self.table_name}
        {self.sql_query}
        """

        print(f'\nLoading dimension table {self.table_name} ...')

        redshift_hook.run(COPY_SQL)

        print(f'\nLoading completed')
