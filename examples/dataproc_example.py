import datetime
import logging

import luigi
from luigi.interface import setup_interface_logging

import target
from luigi_gcloud.dataproc import DataProcPigTask, DataProcSparkTask
from luigi_gcloud.gcore import load_default_client

logger = logging.getLogger('luigi-interface')


class DataProcPigCopy(DataProcPigTask):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail(self.day, 'dp').gcs

    def query_file(self):
        return "examples/queries/pig_foreach_example.pig"

    def name(self):
        return "pig-copy-${year}-${month}-${day}"

    def variables(self):
        return {
            # used in the script
            'in': target.storage_mail_path(self.day).path,
            'out': target.storage_mail(self.day, 'dp').path,
            # used in the name customisation
            'year': self.day.strftime('%Y'),
            'month': self.day.strftime('%m'),
            'day': self.day.strftime('%d'),
        }


class DataProcSparkCopy(DataProcSparkTask):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail(self.day, 'dp').gcs

    def job_file(self):
        return "/luigi-spark/build/libs/luigi-spark-copy-1.0.jar"

    def name(self):
        return "spark-copy-${year}-${month}-${day}"

    def main(self):
        return "luigi.gcloud.spark.Copy"

    def args(self):
        return [
            target.storage_mail_path(self.day).path,
            target.storage_mail(self.day, 'dp').path,
        ]

    def variables(self):
        return {
            # used in the name customisation
            'year': self.day.strftime('%Y'),
            'month': self.day.strftime('%m'),
            'day': self.day.strftime('%d'),
        }


class DataProcExamples(luigi.WrapperTask):
    def requires(self):
        return [
            DataProcPigCopy(datetime.date(2015, 11, 23)),
            DataProcSparkCopy(datetime.date(2015, 11, 24)),
        ]


if __name__ == "__main__":
    setup_interface_logging('examples/logging.ini')
    load_default_client("examples", "examples")
    luigi.run()
