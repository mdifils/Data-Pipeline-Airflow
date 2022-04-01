from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    # background color of the operator in UI
    ui_color = '#358140'
    template_fields = ["copy_option"]

    def __init__(self,
                 aws_conn_id: str,
                 redshift_conn_id: str,
                 s3_bucket: str,
                 s3_prefix: str,
                 table_name: str,
                 copy_option: str,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table_name = table_name
        self.copy_option = copy_option

    def execute(self, context):

        print('\nGetting AWS credentials ...')
        aws_hook = AwsBaseHook(self.aws_conn_id, client_type="iam")
        credentials = aws_hook.get_credentials()

        print('\nGetting redshift connection details ...')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        print(f'\nPreparing to stage data from\
        s3://{self.s3_bucket}/{self.s3_prefix}\
        to {self.table_name} table...')

        COPY_SQL = f"""
        COPY {self.table_name}
        FROM 's3://{self.s3_bucket}/{self.s3_prefix}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        REGION 'us-west-2'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        COMPUPDATE OFF
        {self.copy_option};
        """

        print(f'\nLoading staging table {self.table_name}...')

        redshift_hook.run(COPY_SQL)

        print(f'\nLoading completed')
