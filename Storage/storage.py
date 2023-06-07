import logging
from google.cloud import storage, exceptions


class StorageUtils:
    def __init__(self, service_account=None, logger=None, project_id=None):
        """
        Initialize the StorageUtils class.

        :param service_account: The path to the service account key JSON file.
        :param logger: A custom logger instance to use for logging. If not provided, the default logging module will be used.
        :param project_id: The ID of the Google Cloud project.
        """
        if logger is None:
            self.logger = logging
        else:
            self.logger = logger

        self.project_id = project_id

        self.client = self._create_client(service_account)

    def _create_client(self, service_account):
        """
        Create a Google Cloud Storage client.

        :param service_account: The path to the service account key JSON file.
        """
        if service_account:
            self.client = storage.Client.from_service_account_json(service_account)
        else:
            self.client = storage.Client(project=self.project_id)

    def upload_string(self, bucket, destination_path, content, type="text/csv"):
        """
        Upload a string as a file to Google Cloud Storage.

        :param bucket: The name of the bucket.
        :param destination_path: The destination path of the file in the bucket.
        :param content: The content of the file as a string.
        :param type: The content type of the file (default: "text/csv").
        """
        try:
            bucket = self.client.bucket(bucket)
            blob = bucket.blob(destination_path)
            blob.upload_from_string(content, content_type=type)
            self.logger.info(
                "[Storage.upload_string] File was created in GCS",
                {"bucket": bucket, "filename": destination_path},
            )
        except Exception as error:
            self.logger.error("[Storage.upload_string]", error)

    def upload_file(self, bucket, destination_path, source_file):
        """
        Upload a file to Google Cloud Storage.

        :param bucket: The name of the bucket.
        :param destination_path: The destination path of the file in the bucket.
        :param source_file: The path to the source file.
        """
        try:
            bucket = self.client.bucket(bucket)
            blob = bucket.blob(destination_path)
            blob.upload_from_file(source_file)
            self.logger.info(
                "[Storage.upload_file] File was uploaded to GCS",
                {"bucket": bucket, "filename": destination_path},
            )
        except Exception as error:
            self.logger.error("[Storage.upload_file]", error)

    def upload_filename(self, bucket, destination_path, source_filename):
        """
        Upload a file using the filename to Google Cloud Storage.

        :param bucket: The name of the bucket.
        :param destination_path: The destination path of the file in the bucket.
        :param source_filename: The path to the source file.
        """
        try:
            bucket = self.client.bucket(bucket)
            blob = bucket.blob(destination_path)
            blob.upload_from_filename(source_filename)
            self.logger.info(
                "[Storage.upload_filename] File was uploaded to GCS",
                {"bucket": bucket, "filename": destination_path},
            )
        except Exception as error:
            self.logger.error("[Storage.upload_filename]", error)

    def download_file_content(self, bucket: str, path: str):
        """
        Download the content of a file from Google Cloud Storage.

        :param bucket: The name of the bucket.
        :param path: The path of the file in the bucket.
        :return: The content of the file as a string.
        """
        try:
            bucket = self.client.get_bucket(bucket)
            file_blob = bucket.get_blob(path)
            file_content = file_blob.download_as_text(encoding="utf-8")
            self.logger.info(
                "[Storage.download_file_content] downloaded file",
                {"bucket": bucket, "filename": path},
            )
            return file_content
        except exceptions.NotFound:
            self.logger.error(
                "[Storage.download_file_content] File not found",
                f"gs://{bucket}/{path}",
            )
        except Exception as error:
            self.logger.error("[Storage.download_file_content]", error)
