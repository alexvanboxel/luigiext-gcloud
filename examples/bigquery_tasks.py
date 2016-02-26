import luigi

import target
from luigi_gcloud.bigquery import BigQueryLoadTask, BigQueryTask
from storage_tasks import CopyLocalToStorage


class CopyBigQueryToStorage(BigQueryTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyBigQueryToBigQuery(day=self.day)

    # def output(self):
    #     return target.storage_mail(self.day, 'bq2st').gcs

    def destination(self):
        return target.storage_mail(self.day, 'bq2st').part('part-*.json')

    def variables(self):
        return {
            'temp_dataset': self.get_service_value('tempDataset')
        }

    def query(self):
        return "SELECT * FROM [${temp_dataset}.example_copy]"


class CopyBigQueryToBigQuery(BigQueryTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyStorageToBigQuery(day=self.day)

    def table(self):
        ds = self.get_service_value('tempDataset')
        return ds + ".example_copy"

    def variables(self):
        return {
            'temp_dataset': self.get_service_value('tempDataset')
        }

    def query_file(self):
        return "examples/queries/bq_select_example_mail.bq"


class CopyStorageToBigQuery(BigQueryLoadTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyLocalToStorage(self.day)

    def table(self):
        ds = self.get_service_value('tempDataset')
        return ds + ".example_mail"

    def source(self):
        return target.storage_mail_path(self.day).path

    def schema(self):
        return [
            {"name": "datetime", "type": "timestamp", "mode": "nullable"},
            {"name": "name", "type": "string", "mode": "nullable"},
            {"name": "email", "type": "string", "mode": "nullable"},
            {"name": "campaign", "type": "string", "mode": "nullable"},
            {"name": "id", "type": "string", "integer": "nullable"}
        ]

    def configuration(self):
        return {
            'sourceFormat': "CSV"
        }
