import logging
import time

from luigi_gcloud.gcore import _GCloudTask
from luigi_gcloud.storage import GCSFileSystem

logger = logging.getLogger('luigi-gcloud')

default_pig_udf = None
default_spark_udf = None


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
            if 'CANCELLED' == self.job['status']['state']:
                print(str(self.job))
                logger.warning('DataProc job %s is cancelled', self.job_id)
                logger.warning(self.job['status']['details'])
                logger.debug(str(self.job))
                return False
            if 'DONE' == self.job['status']['state']:
                return True
            logger.debug('DataProc job %s is %s', self.job_id, str(self.job['status']['state']))
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
        return self.client.project_staging() + self.resolved_name() + ".pig"

    def lib_uris(self):
        return []

    def _jar_file_uris(self):
        global default_pig_udf
        artifacts = []
        uris = self.lib_uris()
        artifacts.extend(uris)
        if default_pig_udf is not None:
            if not default_pig_udf.get('append') and len(uris) == 0:
                artifacts.extend(default_pig_udf.get('uris'))
            elif default_pig_udf.get('append'):
                artifacts.extend(default_pig_udf.get('uris'))
        return artifacts

    def run(self):
        self.start_job()
        http = self.client.http_authorized()
        dataproc_api = self.client.dataproc_api(http)
        name = self.job_name()

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
                    "jarFileUris": self._jar_file_uris(),
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


class DataProcSparkTask(_GCloudTask):
    service_name = "dataproc"

    def job_file(self):
        return None

    def job_uri(self):
        return self.client.project_staging() + self.resolved_name() + ".jar"

    def lib_uris(self):
        return []

    def main(self):
        raise NotImplementedError

    def args(self):
        return []

    def _jar_file_uris(self):
        global default_spark_udf
        artifacts = [self.job_uri()]
        uris = self.lib_uris()
        artifacts.extend(uris)
        if default_spark_udf is not None:
            if not default_spark_udf.get('append') and len(uris) == 0:
                artifacts.extend(default_spark_udf.get('uris'))
            elif default_spark_udf.get('append'):
                artifacts.extend(default_spark_udf.get('uris'))
        return artifacts

    def run(self):
        self.start_job()
        http = self.client.http_authorized()
        dataproc_api = self.client.dataproc_api(http)
        name = self.job_name()

        if self.job_file() is not None:
            logger.warning("Copying job artifact to storage. Consider uploading outside of job.")
            fs = GCSFileSystem()
            fs.put(self.get_service_value("basePath", ".") + self.job_file(),
                   self.job_uri())

        job = {
            "job": {
                "reference": {
                    "projectId": self.client.project_id(),
                    "jobId": name,
                },
                "placement": {
                    "clusterName": self.get_service_value("clusterName", "cluster-1")
                },
                "sparkJob": {
                    "mainClass": self.main(),
                    "jarFileUris": self._jar_file_uris(),
                }
            }
        }
        args = self.args()
        if args is not None:
            job["job"]["sparkJob"]["args"] = args

        print(job)
        submitted = _DataProcJob(dataproc_api, self.client.project_id(), job)
        if not submitted.wait_for_done():
            submitted.raise_error("DataProcTask has errors")


def set_default_pig_udf(uris, append=False):
    global default_pig_udf
    default_pig_udf = {
        'append': append,
        'uris': uris
    }


def set_default_spark_udf(uris, append=False):
    global default_spark_udf
    default_spark_udf = {
        'append': append,
        'uris': uris
    }
