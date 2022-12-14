from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class Loader:
    def __init__(
        self,
        university,
        logger=None,
        S3_ID='aws_s3_bucket',
        dest_bucket='alkemy26',
        path='./datasets/'
        ) -> None:

        self.university = university
        self.logger = logger

        self.S3_ID = S3_ID
        self.dest_bucket = dest_bucket
        self.path = path
        self.key = f'{self.university}_process.txt'
        
    def loading(self):
        s3_hook = S3Hook(aws_conn_id=self.S3_ID)

        s3_hook.load_file(
            self.path + self.key,
            key=self.key,
            bucket_name=self.dest_bucket,
            replace=True,
            encrypt=False,
            gzip=False,
        )

    def to_load(self):
        if self.logger:
            self.logger.info('Iniciando proceso de cargue de datos al bucket de S3')

        try:
            self.loading()

            if self.logger:
                self.logger.info('Tarea de carga EXITOSA')

        except Exception as e:
            if self.logger:
                self.logger.info('ERROR al cargar los datos al bucket de S3')
                self.logger.error(e)
