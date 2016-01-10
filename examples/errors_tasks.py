import datetime
import logging

import luigi
from luigi.interface import setup_interface_logging

import target
from luigi_gcloud.bigquery import BigQueryLoadTask
from luigi_gcloud.gcore import load_default_client

logger = logging.getLogger('luigi-interface')


class ErrorStorageToBigQuery(BigQueryLoadTask):
    day = luigi.DateParameter()

    def table(self):
        return "error_out"

    def source(self):
        return target.storage_mail_path(self.day).path

    def schema(self):
        return [
            {"name": "datetime", "type": "timestamp", "mode": "nullable"},
            {"name": "name", "type": "string", "mode": "nullable"},
            {"name": "email", "type": "integer", "mode": "nullable"},
            {"name": "campaign", "type": "string", "mode": "nullable"},
            {"name": "id", "type": "string", "integer": "nullable"}
        ]

    def configuration(self):
        return {
            'sourceFormat': "CSV"
        }


class ErrorAll(luigi.WrapperTask):
    def requires(self):
        return [
            ErrorStorageToBigQuery(datetime.date(2015, 11, 23))
        ]


if __name__ == "__main__":
    setup_interface_logging('examples/logging.ini')
    load_default_client("examples", "examples")
    luigi.run()
