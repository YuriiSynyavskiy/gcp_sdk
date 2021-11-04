import os
from datetime import datetime
from airflow.models.dag import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults


class LocalToGCSOperator(BaseOperator):
    """Upload list of files to a Google Cloud Storage.
    This operator uploads a list of local files to a Google Cloud Storage.
    Files can be generated automatically.
    The local fils is deleted after upload (optional).
    Args:
        local_path: Python list of local file paths (templated)
        delete: should the local file be deleted after upload?
    """
    template_fields = ['previous_file']

    @apply_defaults
    def __init__(
        self,
        bucket_name,
        bucket_folder,
        local_path=None,
        previous_file=None,
        delete=True,
        create_function=None,
        update_function=None,
        logger=None,
        run_id=False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_folder = bucket_folder
        self.local_path = local_path
        self.previous_file = previous_file
        self.delete = delete
        self.create_data = create_function
        self.update_data = update_function
        self.logger = logger
        self.run_id = run_id,
        self.conn = GoogleCloudStorageHook()

    def execute(self, context):
        if self.run_id and self.run_id[0]:
            self.previous_file = None
        if self.previous_file:
            self.download_file()
            if self.logger:
                self.logger.log_struct({
                    'message': f"{datetime.now(tz=None)} Downloading {self.previous_file} for updating rows in job {context['run_id']}",
                    'run_id': context['run_id'],
                    'namespace': self.bucket_folder,
                    'oldest_file': self.previous_file
                }, severity="INFO")

            file_name = self.update_data(self.previous_file.split('/')[-1])
            if self.logger:
                self.logger.log_struct({
                    'message': f"{datetime.now(tz=None)} File {self.previous_file} was updated to {file_name} in job {context['run_id']}",
                    'run_id': context['run_id'],
                    'namespace': self.bucket_folder,
                    'oldest_file': self.previous_file,
                    'new_file': file_name 
                }, severity="INFO")
        else:
            file_name = self.create_data()
            if self.logger:
                self.logger.log_struct({
                    'message': f"{datetime.now(tz=None)} File {file_name} was created in job {context['run_id']}",
                    'run_id': context['run_id'],
                    'namespace': self.bucket_folder,
                    'new_file': file_name 
                }, severity="INFO")

        self.upload_file(file_name)

        if self.logger:
                self.logger.log_struct({
                    'message': f"{datetime.now(tz=None)} File was uploaded to {self.bucket_name}/{self.bucket_folder}/{file_name} in job {context['run_id']}",
                    'run_id': context['run_id'],
                    'namespace': self.bucket_folder,
                    'new_file': file_name 
                }, severity="INFO")
        
        os.remove(file_name)
        if self.previous_file:
            os.remove(self.previous_file.split('/')[-1])


    def download_file(self, local_path=None):

        self.conn.download(
            bucket_name = self.bucket_name,
            object_name = self.previous_file,
            filename = self.previous_file.split('/')[-1]
        )

    def upload_file(self, file_name):

        self.conn.upload(
            bucket_name = self.bucket_name,
            object_name = f'{self.bucket_folder}/{file_name}',
            filename = file_name
        )
        