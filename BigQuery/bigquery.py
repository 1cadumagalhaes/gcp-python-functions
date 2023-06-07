import logging
from google.cloud import bigquery


class BigQueryUtils:
    def __init__(self, service_account=None, logger=None, project_id: str = None):
        """
        Initialize the BigQueryUtils class.

        :param service_account: The path to the service account JSON key file. If not provided, the default credentials will be used.
        :param logger: A custom logger instance to use for logging. If not provided, the default logging module will be used.
        :param project_id: The ID of the Google Cloud project. If not provided, the project ID will be fetched from the default credentials.
        """
        if logger is None:
            self.logger = logging
        else:
            self.logger = logger
        self.project_id = project_id
        self.client = self._create_client(service_account)

    def _create_client(self, service_account):
        """
        Create the BigQuery client.

        :param service_account: The path to the service account JSON key file. If not provided, the default credentials will be used.
        :return: The BigQuery client.
        """
        if service_account:
            self.client = bigquery.Client.from_service_account_json(service_account)
        else:
            self.client = bigquery.Client()

    def table_from_storage(
        self,
        uri: str,
        schema: list,
        dataset: str,
        table: str,
        write_disposition: str = "WRITE_TRUNCATE",
        partition_field: str = None,
    ):
        """
        Load data from Cloud Storage into a BigQuery table.

        :param uri: The URI of the Cloud Storage object to load data from.
        :param schema: The schema of the data being loaded. Each schema field is represented as a dictionary with `name` and `type` keys.
        :param dataset: The BigQuery dataset to load the table into.
        :param table: The name of the BigQuery table.
        :param write_disposition: The write disposition for the load job. Default is "WRITE_TRUNCATE". Other options are "WRITE_APPEND" and "WRITE_EMPTY".
        :param partition_field: The field to use for time-based partitioning of the table. Default is None.
        """
        if partition_field:
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field,
                ),
                write_disposition=write_disposition,
            )
        else:
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=write_disposition,
            )
        table_id = f"{self.project_id}.{dataset}.{table}"

        try:
            load_job = self.client.load_table_from_uri(
                source_uris=uri, destination=table_id, job_config=job_config
            )
            load_job.result()

            table_ref = self.client.get_table(table_id)
            self.logger.info(
                f"[BigQuery.table_from_storage] Loaded {table_ref.num_rows} rows and {len(table_ref.schema)} columns to {table_id}"
            )
        except Exception as error:
            self.logger.error("[BigQuery.table_from_storage]", error)

    def table_from_file(
        self,
        file,
        schema: list,
        dataset: str,
        table: str,
        write_disposition: str = "WRITE_TRUNCATE",
        partition_field: str = None,
    ):
        """
        Load data from a local file into a BigQuery table.

        :param file: The path to the local file to load data from.
        :param schema: The schema of the data being loaded. Each schema field is represented as a dictionary with `name` and `type` keys.
        :param dataset: The BigQuery dataset to load the table into.
        :param table: The name of the BigQuery table.
        :param write_disposition: The write disposition for the load job. Default is "WRITE_TRUNCATE". Other options are "WRITE_APPEND" and "WRITE_EMPTY".
        :param partition_field: The field to use for time-based partitioning of the table. Default is None.
        """
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            ),
            write_disposition=write_disposition,
            schema=schema,
        )
        table_id = f"{self.project_id}.{dataset}.{table}"

        try:
            with open(file, mode="rb") as file_obj:
                load_job = self.client.load_table_from_file(
                    file_obj=file_obj, destination=table_id, job_config=job_config
                )
                load_job.result()

            table_ref = self.client.get_table(table_id)
            self.logger.info(
                f"[BigQuery.table_from_file] Loaded {table_ref.num_rows} rows and {len(table_ref.schema)} columns to {table_id}"
            )
        except Exception as error:
            self.logger.error("[BigQuery.table_from_file]", error)
