import subprocess
from time import sleep

import luigi

from luigiext.gcore import get_default_api


class DataFlowJavaTask(luigi.Task):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        self.api = kwargs.get("api") or get_default_api()
        http = self.api.http()
        self.df = self.api.dataflow_api(http)
        self.gcs = self.api.storage_api(http)
        super(DataFlowJavaTask, self).__init__(*args, **kwargs)

    def requires(self):
        return []

    def output(self):
        return []

    def dataflow(self):
        raise NotImplementedError("subclass should define dataflow")

    def options(self):
        raise NotImplementedError("subclass should define options")

    def params(self):
        raise NotImplementedError("subclass should define params")

    def run(self):
        cmd = self._build_cmd()
        self._execute_track(cmd)

    def _execute_track(self, cmd):
        print(cmd)
        proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, )

        while proc.poll() is None:
            print "Waiting for Google Cloud DataFlow to complete."
            sleep(5)

    def _build_cmd(self):
        options = self.options()

        print(options)
        command = [
            "java",
            "-jar",
            options.get("basePath", ".") + self.dataflow(),
            "--project=" + (options.get("projectId") or self.api.project_id()),
            "--zone=" + (options.get("zone") or self.api.get("dataflow.zone")),
            "--stagingLocation=" + (options.get("stagingLocation") or self.api.get("dataflow.staging")),
            "--runner=" + (options.get("runner") or self.api.get("dataflow.runner")),
            "--autoscalingAlgorithm=" + options.get("autoscalingAlgorithm", "BASIC"),
            "--maxNumWorkers=" + options.get("maxNumWorkers", "50")
        ]

        for attr, value in self.params().iteritems():
            command.append("--" + attr + "=" + value)

        return command
