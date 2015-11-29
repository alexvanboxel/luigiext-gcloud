import datetime
import logging

import luigi
from luigi.interface import setup_interface_logging

import target
from luigi_gcloud.bigquery import BigQueryLoadTask, BigQueryTask
from luigi_gcloud.dataflow import DataFlowJavaTask
from luigi_gcloud.gcore import load_default_client, load_query_file
from luigi_gcloud.storage import GCSFileSystem

logger = logging.getLogger('luigi-interface')


class CopyBigQueryToStorage(BigQueryTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyBigQueryToBigQuery(day=self.day)

    # def output(self):
    #     return target.storage_mail(self.day, 'bq2st').gcs

    def destination(self):
        return target.storage_mail(self.day, 'bq2st').part('part-*.json')

    def params(self):
        return {
            'temp_dataset': self.get_config_value('tempDataset')
        }

    def query(self):
        return load_query_file("examples/queries/bq_select_example_copy.bq")


class CopyBigQueryToBigQuery(BigQueryTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyStorageToBigQuery(day=self.day)

    def table(self):
        return "example_copy"

    def params(self):
        return {
            'temp_dataset': self.get_config_value('tempDataset')
        }

    def query(self):
        return load_query_file("examples/queries/bq_select_example_mail.bq")


class CopyStorageToBigQuery(BigQueryLoadTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyLocalToStorage(self.day)

    def table(self):
        return "example_mail"

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


class CopyLocalToStorage(luigi.Task):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail_path(self.day).gcs

    def run(self):
        fs = GCSFileSystem()
        fs.put(target.local_mail_path(self.day),
               target.storage_mail_path(self.day).path)


class CopyAllLocalToStorage(luigi.WrapperTask):
    def requires(self):
        return [
            CopyLocalToStorage(datetime.date(2015, 11, 23)),
            CopyLocalToStorage(datetime.date(2015, 11, 24)),
            CopyLocalToStorage(datetime.date(2015, 11, 25)),
            CopyLocalToStorage(datetime.date(2015, 11, 26)),
            CopyLocalToStorage(datetime.date(2015, 11, 27)),
            CopyLocalToStorage(datetime.date(2015, 11, 28)),
            CopyLocalToStorage(datetime.date(2015, 11, 29))
        ]


class CopyViaDataFlowToStorage(DataFlowJavaTask):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail(self.day, 'dump').gcs

    def requires(self):
        return CopyLocalToStorage(self.day)

    def dataflow(self):
        return "/pipeline/build/libs/pipeline-copy-1.0.jar"

    def configuration(self):
        return {
            "runner": "DataflowPipelineRunner",
            "autoscalingAlgorithm": "BASIC",
            "maxNumWorkers": "80"
        }

    def params(self):
        return {
            'in': target.storage_mail_path(self.day).path,
            'out': target.storage_mail(self.day, 'df').path
        }


class ExamplesAll(luigi.WrapperTask):
    def requires(self):
        return [
            CopyAllLocalToStorage(),
            CopyViaDataFlowToStorage(datetime.date(2015, 11, 23)),
            CopyBigQueryToStorage(datetime.date(2015, 11, 24))
        ]


if __name__ == "__main__":
    setup_interface_logging('examples/logging.ini')
    load_default_client("examples", "examples")
    luigi.run()
