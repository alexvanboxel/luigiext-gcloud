import logging
import select
import subprocess

import luigi

from luigi_gcloud.gcore import get_default_api

logger = logging.getLogger('luigi-interface')


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

    def configuration(self):
        raise NotImplementedError("subclass should define configuration")

    def params(self):
        raise NotImplementedError("subclass should define params")

    def run(self):
        cmd = self._build_cmd()
        self._execute_track(cmd)

    def _execute_track(self, cmd):
        print(cmd)
        proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, )
        reads = [proc.stderr.fileno(), proc.stdout.fileno()]

        print "Start waiting for Google Cloud DataFlow to complete."
        while proc.poll() is None:
            ret = select.select(reads, [], [], 5)
            if ret is not None:
                for fd in ret[0]:
                    if fd == proc.stderr.fileno():
                        line = proc.stderr.readline()
                        print(line)
                    if fd == proc.stdout.fileno():
                        line = proc.stdout.readline()
                        print(line)
            else:
                print "Waiting for Google Cloud DataFlow to complete."

        self._success()

    def _success(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        return

    def _build_cmd(self):
        config = self.configuration()

        print(config)
        command = [
            "java",
            "-jar",
            self.api.get(config, "dataflow", "basePath") + self.dataflow(),
            "--project=" + (config.get("projectId") or self.api.project_id()),
            "--zone=" + self.api.get(config, "dataflow", "zone"),
            "--stagingLocation=" + self.api.get(config, "dataflow", "stagingLocation"),
            "--runner=" + self.api.get(config, "dataflow", "runner"),
            "--autoscalingAlgorithm=" + self.api.get(config, "dataflow", "autoscalingAlgorithm"),
            "--maxNumWorkers=" + self.api.get(config, "dataflow", "maxNumWorkers")
        ]

        for attr, value in self.params().iteritems():
            command.append("--" + attr + "=" + value)

        return command
