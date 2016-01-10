import logging

import luigi

import target
from luigi_gcloud.dataproc import DataProcPigTask, DataProcSparkTask
from storage_tasks import CopyLocalToStorage

logger = logging.getLogger('luigi-interface')


class DataProcPigCopy(DataProcPigTask):
    day = luigi.DateParameter()

    def requires(self):
        return CopyLocalToStorage(self.day)

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

    def requires(self):
        return CopyLocalToStorage(self.day)

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
