from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator


class DataQualityOperator(BaseOperator):
    # background color of the operator in UI
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 check_dict: dict,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_dict = check_dict

    def execute(self, context):

        print('\nGetting redshift connection details ...')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        error_count = 0

        print("\nChecking Tables size ...")
        for check in self.check_dict['check_table_size']:
            record = redshift_hook.get_records(check)[0][0]
            if record == 0:
                print(f"\nThe following check failed:\n {check}")
                error_count += 1

        print("\nChecking Null Values ...")
        for check in self.check_dict['check_null_value']:
            record = redshift_hook.get_records(check)[0][0]
            if record != 0:
                print(f"\nThe following check failed:\n {check}")
                error_count += 1

        if error_count > 0:
            raise ValueError('Data quality check failed')
        else:
            print("All data quality checks passed")
