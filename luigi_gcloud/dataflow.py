import logging
import select
import subprocess
import time

import luigi

from luigi_gcloud.gcore import get_default_api

logger = logging.getLogger('luigi-gcloud')


class _DataflowJob:
    def _get_job(self):
        job = self.dataflow.projects().jobs().get(projectId=self.project_number,
                                                  jobId=self.job_id).execute()
        if 'currentState' in job:
            logger.info('Google Cloud DataFlow job %s is %s', str(job['name']), str(job['currentState']))
        else:
            logger.info('Google Cloud DataFlow with job_id %s has name %s', self.job_id, str(job['name']))
        return job

    def wait_for_done(self):
        while True:
            if 'currentState' in self.job:
                if 'JOB_STATE_DONE' == self.job['currentState']:
                    return True
                elif 'JOB_STATE_FAILED' == self.job['currentState']:
                    raise Exception("Google Cloud Dataflow job " + str(self.job['name']) + " has failed.")
                elif 'JOB_STATE_CANCELLED' == self.job['currentState']:
                    raise Exception("Google Cloud Dataflow job " + str(self.job['name']) + " was cancelled.")
                elif 'JOB_STATE_RUNNING' == self.job['currentState']:
                    time.sleep(10)
                else:
                    logger.debug(str(self.job))
                    raise Exception("Google Cloud Dataflow job " + str(self.job['name']) + " was unknown state: " + str(
                        self.job['currentState']))
            else:
                time.sleep(15)

            self.job = self._get_job()

    def get(self):
        return self.job

    def __init__(self, dataflow, project_number, job_id):
        self.dataflow = dataflow
        self.project_number = project_number
        self.job_id = job_id
        self.job = self._get_job()


class _DataflowJava:
    def _line(self, fd):
        if fd == self.proc.stderr.fileno():
            return self.proc.stderr.readline()
        if fd == self.proc.stdout.fileno():
            return self.proc.stdout.readline()
        return None

    def _extract_job(self, line):
        if line is not None:
            if line.startswith("Submitted job: "):
                return line[15:-1]
        return None

    def wait_for_done(self):
        reads = [self.proc.stderr.fileno(), self.proc.stdout.fileno()]
        logger.info("Start waiting for DataFlow process to complete.")
        while self.proc.poll() is None:
            ret = select.select(reads, [], [], 5)
            if ret is not None:
                for fd in ret[0]:
                    line = self._line(fd)
                    self.job_id = self._extract_job(line)
                    if self.job_id is not None:
                        return self.job_id
                    else:
                        logger.debug(line[:-1])
            else:
                logger.info("Waiting for DataFlow process to complete.")

    def get(self):
        return self.job_id

    def __init__(self, cmd):
        self.job_id = None
        self.proc = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


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
        logger.debug("DataFlow process: " + str(cmd))
        job_id = _DataflowJava(cmd).wait_for_done()
        _DataflowJob(self.df, self.api.project_id(), job_id).wait_for_done()
        self._success()

    def _success(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        return

    def _build_cmd(self):
        config = self.configuration()
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
