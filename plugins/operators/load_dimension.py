from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator


class LoadDimensionOperator(BaseOperator):
    # background color of the operator in UI
    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id: str,
                 table_name: str,
                 sql_query: str,
                 truncate_table=False,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):

        print('\nGetting redshift connection details ...')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_table:
            print(f"\nTruncating dimension table: {self.table_name}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")

        COPY_SQL = f"""
        INSERT INTO {self.table_name}
        {self.sql_query}
        """

        print(f'\nLoading dimension table {self.table_name} ...')

        redshift_hook.run(COPY_SQL)

        print(f'\nLoading completed')
