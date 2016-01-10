import luigi

import target
from luigi_gcloud.dataflow import DataFlowJavaTask
from storage_tasks import CopyLocalToStorage


class CopyViaDataFlowToStorage(DataFlowJavaTask):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail(self.day, 'dump').gcs

    def requires(self):
        return CopyLocalToStorage(self.day)

    def dataflow(self):
        return "/luigi-dataflow/pipeline/build/libs/pipeline-copy-1.0.jar"

    def configuration(self):
        return {
            "runner": "DataflowPipelineRunner",
            "autoscalingAlgorithm": "BASIC",
            "maxNumWorkers": "3"
        }

    def variables(self):
        return {
            'in': target.storage_mail_path(self.day).path,
            'out': target.storage_mail(self.day, 'df').path
        }
