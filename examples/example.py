import datetime
import logging

import luigi
import target
from luigi.interface import setup_interface_logging

from luigi_gcloud import gcore
from luigi_gcloud.dataflow import DataFlowJavaTask
from luigi_gcloud.storage import GCSFlagTarget, GCSFileSystem

logger = logging.getLogger('luigi-interface')


class CopyLocalToStorage(luigi.Task):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail_path(self.day).gcs

    def run(self):
        fs = GCSFileSystem()
        fs.put(target.local_mail_path(self.day),
               target.storage_mail_path(self.day).output)


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
        return target.storage_mail_dump(self.day).gcs

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
            'in': target.storage_mail_path(self.day).output,
            'out': target.storage_mail_dump(self.day).output
        }


class ExamplesAll(luigi.WrapperTask):
    def requires(self):
        return [
            CopyAllLocalToStorage(),
            CopyViaDataFlowToStorage(datetime.date(2015, 11, 23))
        ]

if __name__ == "__main__":
    setup_interface_logging('examples/logging.ini')
    gcore.load_default_client("examples", "examples")
    luigi.run()
