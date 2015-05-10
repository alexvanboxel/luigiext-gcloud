import logging
from string import Template

import luigi


# from luigi import six

from luigi.target import FileSystemException
from googleapiclient.errors import HttpError
import time
from luigiext.gcore import GCloudClient, get_default_api

logger = logging.getLogger('luigi-interface')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")


def _wait_for_jobid_complete(api, jobId):
    while True:
        status = api.bigquery_api().jobs().get(projectId=api.project_number(), jobId=jobId).execute()
        if 'DONE' == status['status']['state']:
            print "Done !" + str(status)
            return True
        print 'Waiting for export to complete..'
        time.sleep(5)


def _wait_for_job_complete(api, job):
    print(job)
    return _wait_for_jobid_complete(api, jobId=job['jobReference']['jobId'])


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
        print("REQ")
        print(job)
        self.bq = bq
        self.project_number = project_number
        self.job = self.bq.jobs().insert(projectId=self.project_number, body=job).execute()
        print("RES")
        print(self.job)
        self.job_id = self.job['jobReference']['jobId']

    def wait_for_done(self):

        while True:
            self.job = self.bq.jobs().get(
                projectId=self.project_number,
                jobId=self.job_id).execute()
            print("LOOP")
            print(self.job)
            if 'DONE' == self.job['status']['state']:
                return True
            print 'Waiting...'
            time.sleep(5)

    def get(self):
        return self.job


class BigQueryTarget(luigi.Target):
    def __init__(self, table, query=None, client=GCloudClient()):
        self.client = client
        self.table = _split_tablename(table)
        print "TABLE SPLIT: " + str(self.table)
        self.query = query

    def query(self):
        raise NotImplementedError

    def exists(self):
        print "checking existence"
        tables = self.client.bigquery_api().tables()
        print "tables"
        try:
            print "TABLE GET : " + str(self.table)
            table = tables.get(projectId=self.table['projectId'], datasetId=self.table['datasetId'],
                               tableId=self.table['tableId']).execute()
            print "table"
            jobs = self.client.bigquery_api().jobs()
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

            insertJob = jobs.insert(projectId=self.table['projectId'], body=job).execute()
            while True:
                status = jobs.get(projectId=self.table['projectId'], jobId=insertJob['jobReference']['jobId']).execute()
                print status
                if 'DONE' == status['status']['state']:
                    print "Done exporting!" + str(status)
                    result = jobs.getQueryResults(projectId=self.table['projectId'],
                                                  jobId=insertJob['jobReference']['jobId']).execute()
                    print "RESULT : " + str(result)
                    print "VALUE : " + str(result['rows'][0]['f'][0]['v'])
                    return result['rows'][0]['f'][0]['v'] in ['true', 'True']
                print 'Waiting for export to complete..'
                time.sleep(5)
            return False
        except HttpError as err:
            print "HttpError, could be table not found."
            return False


class BqTableExtractTask(luigi.Task):
    def run(self):
        logger.info("Running BqTableExtractTask")


class BqTableLoadTask(luigi.Task):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        self.api = kwargs.get("api") or get_default_api()
        http = self.api.http()
        self._gbq = self.api.bigquery_api(http)
        self._gcs = self.api.storage_api(http)
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

    # def configure_source_format(self):
    #     """Source Format default CSV. Possible values CSV,DATASTORE_BACKUP,NEWLINE_DELIMITED_JSON"""
    #     return "CSV"

    def run(self):
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
                    'writeDisposition': 'WRITE_APPEND',
                    'sourceUris': [self.source()],
                    'maxBadRecords': 1000,
                    'schema': {
                        'fields': self.schema()
                    }
                }
            }
        }
        print "JOB : " + str(job)
        insert_job = jobs.insert(projectId=table['projectId'], body=job).execute()
        if _wait_for_job_complete(self.api, job=insert_job):
            marker = self.output()
            if callable(getattr(marker, "touch")):
                logger.info("Writing marker file " + str(marker))
                marker.touch()
            return


class BqTableCopyTask(luigi.Task):
    def run(self):
        logger.info("Running BqTableCopyTask")


class BqQueryTask(luigi.Task):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        api = kwargs.get("api") or get_default_api()
        self.project_number = api.project_number()
        http = api.http()
        self._gbq = api.bigquery_api(http)
        self._gcs = api.storage_api(http)
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
            "writeDisposition": "WRITE_EMPTY"
        }

    def params(self):
        return {}

    def _success(self):
        marker = self.output()
        if callable(getattr(marker, "touch")):
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
                            'destinationFormat': 'AVRO'
                        }
                    }
                }
                extract_job = _BqJob(self._gbq, self.project_number, job=job)
                if extract_job.wait_for_done():
                    return
