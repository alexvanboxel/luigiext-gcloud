import logging
from datetime import datetime
from string import Template

import luigi
# from luigi import six

from googleapiclient.errors import HttpError
import time
from luigi_gcloud.gcore import get_default_client

logger = logging.getLogger('luigi-gcloud')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")


def _split_tablename(name):
    print name
    cmpt = name.split(':')
    if len(cmpt) == 1:
        projectId = 0
        rest = cmpt[0]
    elif len(cmpt) == 2:
        projectId = cmpt[0]
        rest = cmpt[1]
    else:
        raise RuntimeError

    cmpt = rest.split('.')
    if len(cmpt) == 2:
        datasetId = cmpt[0]
        tableId = cmpt[1]
    else:
        raise RuntimeError

    return {
        'projectId': projectId,
        'datasetId': datasetId,
        'tableId': tableId
    }


class _BqJob:
    def __init__(self, bq, project_number, job):
        self.bq = bq
        self.project_number = project_number
        self.job = self.bq.jobs().insert(projectId=self.project_number, body=job).execute()
        self.job_id = self.job['jobReference']['jobId']
        logger.info('BigQuery job %s is %s', self.job_id, str(self.job['status']))

    def wait_for_done(self):

        while True:
            self.job = self.bq.jobs().get(
                projectId=self.project_number,
                jobId=self.job_id).execute()
            if 'DONE' == self.job['status']['state']:
                if self.job['status'].has_key('errors'):
                    logger.error('BigQuery job %s has errors', self.job_id)
                    logger.error(str(self.job['status']['errors']))
                    logger.debug(str(self.job))
                    return False
                else:
                    logger.info('BigQuery job %s is DONE', self.job_id)
                    return True
                return True
            logger.info('BigQuery job %s is %s', self.job_id, str(self.job['status']))
            time.sleep(5)

    def get(self):
        return self.job


class BigQueryTarget(luigi.Target):
    def __init__(self, table, query=None, client=get_default_client()):
        self.api = client
        self.table = _split_tablename(table)
        logger.debug("TABLE SPLIT: " + str(self.table))
        self.query = query
        http = self.api.http_authorized()
        self._gbq = self.api.bigquery_api(http)

    def query(self):
        raise NotImplementedError

    def exists(self):
        print "checking existence"
        tables = self._gbq.tables()
        print "tables"
        try:
            print "TABLE GET : " + str(self.table)
            table = tables.get(projectId=self.table['projectId'], datasetId=self.table['datasetId'],
                               tableId=self.table['tableId']).execute()
            print "table"
            jobs = self._gbq.jobs()
            job = {
                'projectId': self.table['projectId'],
                'configuration': {
                    'query': {
                        'useQueryCache': 'True',
                        'query': self.query
                    }
                }
            }
            logger.debug(table)
            if self.query is None:
                return True

            insert_job = _BqJob(self._gbq, self.table['projectId'], job=job)
            if insert_job.wait_for_done():
                result = insert_job.get()
                result = jobs.getQueryResults(projectId=self.table['projectId'],
                                              jobId=result['jobReference']['jobId']).execute()
                print "RESULT : " + str(result)
                print "VALUE : " + str(result['rows'][0]['f'][0]['v'])
                return result['rows'][0]['f'][0]['v'] in ['true', 'True']
        except HttpError as err:
            print "HttpError, could be table not found."
            print str(err)
            return False


class BqTableExtractTask(luigi.Task):
    def run(self):
        logger.info("Running BqTableExtractTask")


class BqTableLoadTask(luigi.Task):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        self.client = kwargs.get("client") or get_default_client()
        http = self.client.http_authorized()
        self._gbq = self.client.bigquery_api(http)
        self._gcs = self.client.storage_api(http)
        super(BqTableLoadTask, self).__init__(*args, **kwargs)

    def schema(self):
        raise NotImplementedError

    def source(self):
        raise NotImplementedError

    def table(self):
        """
        Format 123456789:DataSet.Table
        :return:
        """
        raise NotImplementedError

    def configuration(self):
        """
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
        :return:
        """
        return {
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND",
        }

    def run(self):
        configuration = self.configuration()
        table = _split_tablename(self.table())
        jobs = self._gbq.jobs()
        job = {
            'projectId': table['projectId'],
            'configuration': {
                'load': {
                    # 'quote':'',
                    'destinationTable': {
                        'projectId': table['projectId'],
                        'datasetId': table['datasetId'],
                        'tableId': table['tableId']
                    },
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON',
                    'createDisposition': configuration.get("createDisposition", "CREATE_IF_NEEDED"),
                    'writeDisposition': configuration.get("writeDisposition", "WRITE_APPEND"),
                    'sourceUris': [self.source()],
                    'maxBadRecords': 1000,
                    'schema': {
                        'fields': self.schema()
                    }
                }
            }
        }
        load_job = _BqJob(self._gbq, table['projectId'], job=job)
        if load_job.wait_for_done():
            marker = self.output()
            if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
                logger.info("Writing marker file " + str(marker))
                marker.touch()
            return


class BqTableCopyTask(luigi.Task):
    def run(self):
        logger.info("Running BqTableCopyTask")


class BqQueryTask(luigi.Task):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        self.client = kwargs.get("client") or get_default_client()
        self.project_number = self.client.project_number()
        http = self.client.http_authorized()
        self._gbq = self.client.bigquery_api(http)
        self._gcs = self.client.storage_api(http)
        super(BqQueryTask, self).__init__(*args, **kwargs)

    def schema(self):
        return None

    def query(self):
        raise NotImplementedError

    def table(self):
        """
        Format 123456789:DataSet.Table
        :return:
        """
        return None

    def destination(self):
        return None

    def configuration(self):
        """
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
        :return:
        """
        return {
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_EMPTY",
            "destinationFormat": "AVRO"
        }

    def params(self):
        return {}

    def _success(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        return

    def run(self):
        configuration = self.configuration()
        query = Template(self.query()).substitute(self.params())
        job = {
            'projectId': self.project_number,
            "configuration": {
                "query": {
                    "query": query,
                }
            }
        }
        schema = self.schema()
        if schema is not None:
            job["configuration"]["schema"] = {
                'fields': schema
            }
        if self.table() is None and self.destination() is None:
            raise RuntimeError("At least table or destination need to be supplied.")
        if self.table() is not None and self.destination() is not None:
            raise RuntimeError("Set either table or destination.")

        if self.table() is not None:
            table = _split_tablename(self.table())
            job["configuration"]["query"]["destinationTable"] = {
                'projectId': table['projectId'],
                'datasetId': table['datasetId'],
                'tableId': table['tableId']
            }
            job["configuration"]["query"]["createDisposition"] = \
                configuration.get("createDisposition", "CREATE_IF_NEEDED")
            job["configuration"]["query"]["writeDisposition"] = \
                configuration.get("writeDisposition", "WRITE_APPEND")
            job["configuration"]["query"]["allowLargeResults"] = \
                configuration.get("allowLargeResults", "false")
            insert_job = _BqJob(self._gbq, self.project_number, job=job)
            if insert_job.wait_for_done():
                self._success()

        if self.destination() is not None:
            large = configuration.get("allowLargeResults", "false")
            if str(large) in ("true", "1", "True"):
                # This is a workaround for a BigQuery limitation, if the result is to large
                # we need to set a destination table and do allowLargeResults
                # we need a DataSet that has an expiration to make it work properly in production
                job["configuration"]["query"]["destinationTable"] = {
                    'projectId': self.project_number,
                    'datasetId': self.client.get(configuration, "bigquery", "tempDataset"),
                    'tableId': datetime.now().strftime("xlbq_%Y%m%d%H%M%S")
                }
                job["configuration"]["query"]["allowLargeResults"] = "true"

            insert_job = _BqJob(self._gbq, self.project_number, job=job)
            if insert_job.wait_for_done():
                print(insert_job)
                result = insert_job.get()

                job = {
                    'projectId': self.project_number,
                    'configuration': {
                        'extract': {
                            'sourceTable': {
                                'projectId': result["configuration"]["query"]["destinationTable"]["projectId"],
                                'datasetId': result["configuration"]["query"]["destinationTable"]["datasetId"],
                                'tableId': result["configuration"]["query"]["destinationTable"]["tableId"]
                            },
                            'destinationUris': [
                                self.destination()
                            ],
                            'destinationFormat': configuration.get("destinationFormat", "AVRO")
                        }
                    }
                }
                extract_job = _BqJob(self._gbq, self.project_number, job=job)
                if extract_job.wait_for_done():
                    self._success()
