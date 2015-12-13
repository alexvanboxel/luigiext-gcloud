import logging
import select
import subprocess

import luigi
import time

from luigi_gcloud.gcore import get_default_client, _GCloudTask
from luigi_gcloud.storage import GCSFileSystem

logger = logging.getLogger('luigi-gcloud')



class _DataProcJob:
    def __init__(self, dataproc_api, project_id, job):
        self.dataproc_api = dataproc_api
        self.project_id = project_id
        self.job = dataproc_api.projects().jobs().submit(
            projectId=self.project_id,
            body=job).execute()
        self.job_id = self.job['reference']['jobId']
        logger.info('DataProc job %s is %s', self.job_id, str(self.job['status']['state']))

    def wait_for_done(self):
        while True:
            self.job = self.dataproc_api.projects().jobs().get(
                projectId=self.project_id,
                jobId=self.job_id).execute()
            if 'ERROR' == self.job['status']['state']:
                print(str(self.job))
                logger.error('DataProc job %s has errors', self.job_id)
                logger.error(self.job['status']['details'])
                logger.debug(str(self.job))
                return False
            if 'DONE' == self.job['status']['state']:
                return True
            logger.info('DataProc job %s is %s', self.job_id, str(self.job['status']['state']))
            time.sleep(5)

    def raise_error(self, message=None):
        if 'ERROR' == self.job['status']['state']:
            if message is None:
                message = "Google DataProc job has error"
            raise Exception(message + ": " + str(self.job['status']['details']))

    def get(self):
        return self.job


class DataProcPigTask(_GCloudTask):
    service_name = "dataproc"

    def query_file(self):
        return None

    def query_uri(self):
        return self.client.staging() + self.resolved_name() + ".pig"

    def run(self):
        http = self.client.http_authorized()
        dataproc_api = self.client.dataproc_api(http)
        name = self.resolved_name()

        if self.query_file() is not None:
            fs = GCSFileSystem()
            fs.put(self.query_file(),
                   self.query_uri())

        job = {
            "job": {
                "reference": {
                    "projectId": self.client.project_id(),
                    "jobId": name,
                },
                "placement": {
                    "clusterName": self.get_service_value("clusterName", "cluster-1")
                },
                "pigJob": {
                    "continueOnFailure": False,
                    "queryFileUri": self.query_uri(),
                }
            }
        }
        variables = self.variables()
        if variables is not None:
            job["job"]["pigJob"]["scriptVariables"] = variables

        print(job)
        submitted = _DataProcJob(dataproc_api, self.client.project_id(), job)
        if not submitted.wait_for_done():
            submitted.raise_error("DataProcTask has errors")
