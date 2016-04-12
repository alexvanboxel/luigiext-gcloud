import logging
from datetime import datetime
from string import Template

import luigi
# from luigi import six

from googleapiclient.errors import HttpError
import time
from luigi_gcloud.gcore import get_default_client, _GCloudTask
from luigi_gcloud.storage import GCSTarget

logger = logging.getLogger('luigi-gcloud')

default_bigquery_udf = None


def _table_with_default(name, client):
    return _split_tablename(name, client.project_id())


def _split_tablename(name, default_project=None):
    cmpt = name.split(':')
    if len(cmpt) == 1:
        project_id = default_project
        rest = cmpt[0]
    elif len(cmpt) == 2:
        project_id = cmpt[0]
        rest = cmpt[1]
    else:
        raise RuntimeError

    cmpt = rest.split('.')
    if len(cmpt) == 2:
        dataset_id = cmpt[0]
        table_id = cmpt[1]
    else:
        raise RuntimeError

    return {
        'projectId': project_id,
        'datasetId': dataset_id,
        'tableId': table_id
    }


class _BqJob:
    def __init__(self, bq, project_id, job):
        self.bq = bq
        self.project_id = project_id
        self.job = self.bq.jobs().insert(projectId=self.project_id, body=job).execute()
        self.job_id = self.job['jobReference']['jobId']
        logger.info('BigQuery job %s is %s', self.job_id, str(self.job['status']))

    def wait_for_done(self):

        while True:
            self.job = self.bq.jobs().get(
                projectId=self.project_id,
                jobId=self.job_id).execute()
            if 'DONE' == self.job['status']['state']:
                if 'errors' in self.job['status']:
                    logger.warning('BigQuery job %s has errors', self.job_id)
                    logger.debug(str(self.job))
                    return False
                else:
                    logger.info('BigQuery job %s is DONE', self.job_id)
                    return True
            logger.debug('BigQuery job %s is %s', self.job_id, str(self.job['status']))
            time.sleep(5)

    def raise_error(self, message):
        if 'errors' in self.job['status']:
            if message is None:
                message = "Google BigQuery job has error"
            raise Exception(message + ": " + str(self.job['status']['errors']))

    def get(self):
        return self.job


class BigQueryTarget(luigi.Target):
    def __init__(self, table, query=None, client=None):
        self.client = client or get_default_client()
        self.table = _table_with_default(table, self.client)
        self.query = query
        http = self.client.http_authorized()
        self.bigquery_api = self.client.bigquery_api(http)

    def query(self):
        raise NotImplementedError

    def exists(self):
        tables = self.bigquery_api.tables()
        try:
            table = tables.get(projectId=self.table['projectId'], datasetId=self.table['datasetId'],
                               tableId=self.table['tableId']).execute()
            jobs = self.bigquery_api.jobs()
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

            insert_job = _BqJob(self.bigquery_api, self.table['projectId'], job=job)
            if insert_job.wait_for_done():
                result = insert_job.get()
                result = jobs.getQueryResults(projectId=self.table['projectId'],
                                              jobId=result['jobReference']['jobId']).execute()
                return result['rows'][0]['f'][0]['v'] in ['true', 'True']
        except HttpError as err:
            if err.resp.status == 404:
                return False
            logger.error("HttpError, when checking existence of " + self.table['projectId'] + ":" + self.table[
                'datasetId'] + "." + self.table['tableId'] + ".")
            logger.debug(str(err))
            return False


class _BqTask(_GCloudTask):
    service_name = 'bigquery'
    client = None
    bigquery_api = None

    def __init__(self, *args, **kwargs):
        self.client = kwargs.get("client") or get_default_client()
        http = self.client.http_authorized()
        self.bigquery_api = self.client.bigquery_api(http)
        super(_BqTask, self).__init__(*args, **kwargs)


class BigQueryLoadTask(_BqTask):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        super(BigQueryLoadTask, self).__init__(*args, **kwargs)

    def output(self):
        return BigQueryTarget(self.table(), None, self.client)

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

    def run(self):
        table = _table_with_default(self.table(), self.client)
        # https://cloud.google.com/bigquery/docs/reference/v2/jobs
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
                    'sourceFormat': self.get_service_value("sourceFormat", "NEWLINE_DELIMITED_JSON"),
                    'createDisposition': self.get_service_value("createDisposition", "CREATE_IF_NEEDED"),
                    'writeDisposition': self.get_service_value("writeDisposition", "WRITE_APPEND"),
                    'sourceUris': [self.source()],
                    'maxBadRecords': 1000,
                    'schema': {
                        'fields': self.schema()
                    }
                }
            }
        }
        load_job = _BqJob(self.bigquery_api, table['projectId'], job=job)
        if load_job.wait_for_done():
            marker = self.output()
            if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
                logger.info("Writing marker file " + str(marker))
                marker.touch()
            return
        else:
            load_job.raise_error("Error loading data in Google BigQuery")


class BigQueryTask(_BqTask):
    """ Query a BigQuery table into another table or storage file """

    def __init__(self, *args, **kwargs):
        super(BigQueryTask, self).__init__(*args, **kwargs)

    def output(self):
        if self.table() is not None:
            return BigQueryTarget(self.table())
        if self.destination() is not None:
            destination = self.destination()
            if destination.find('*') != -1:
                destination = destination[:destination.rindex('/') + 1]
            return GCSTarget(destination)

    def schema(self):
        return None

    def query(self):
        with open(self.query_file(), 'r') as f:
            content = f.read()
        return content

    def query_file(self):
        return NotImplementedError

    def table(self):
        """
        Format 123456789:DataSet.Table
        :return:
        """
        return None

    def destination(self):
        return None

    def lib_uris(self):
        return []

    def _success(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        return

    def _js_file_uris(self):
        global default_bigquery_udf
        artifacts = []
        uris = self.lib_uris()
        artifacts.extend(uris)
        if default_bigquery_udf is not None:
            if not default_bigquery_udf.get('append') and len(uris) == 0:
                artifacts.extend(default_bigquery_udf.get('uris'))
            elif default_bigquery_udf.get('append'):
                artifacts.extend(default_bigquery_udf.get('uris'))
        return artifacts

    def run(self):
        query = Template(self.query()).substitute(self.variables())
        job = {
            'projectId': self.client.project_id(),
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

        udf = self._js_file_uris()
        if len(udf) > 0:
            job["configuration"]["query"]["userDefinedFunctionResources"] = []
            for uri in udf:
                job["configuration"]["query"]["userDefinedFunctionResources"].append(
                    {
                        "resourceUri": uri
                    }
                )

        if self.table() is None and self.destination() is None:
            raise RuntimeError("At least table or destination need to be supplied.")
        if self.table() is not None and self.destination() is not None:
            raise RuntimeError("Set either table or destination.")

        if self.table() is not None:
            table = _table_with_default(self.table(), self.client)
            job["configuration"]["query"]["destinationTable"] = {
                'projectId': table['projectId'],
                'datasetId': table['datasetId'],
                'tableId': table['tableId']
            }
            job["configuration"]["query"]["createDisposition"] = \
                self.get_service_value("createDisposition", "CREATE_IF_NEEDED")
            job["configuration"]["query"]["writeDisposition"] = \
                self.get_service_value("writeDisposition", "WRITE_APPEND")
            job["configuration"]["query"]["allowLargeResults"] = \
                self.get_service_value("allowLargeResults", "false")

            insert_job = _BqJob(self.bigquery_api, self.client.project_id(), job=job)
            if insert_job.wait_for_done():
                self._success()
            else:
                insert_job.raise_error("Error executing query in Google BigQuery")

        if self.destination() is not None:
            large = self.get_service_value("allowLargeResults", "false")
            if str(large) in ("true", "1", "True"):
                # This is a workaround for a BigQuery limitation, if the result is to large
                # we need to set a destination table and do allowLargeResults
                # we need a DataSet that has an expiration to make it work properly in production
                job["configuration"]["query"]["destinationTable"] = {
                    'projectId': self.client.project_id(),
                    'datasetId': self.get_service_value("tempDataset", "work_temp"),
                    'tableId': datetime.now().strftime("xlbq_%Y%m%d%H%M%S")
                }
                job["configuration"]["query"]["allowLargeResults"] = "true"

            insert_job = _BqJob(self.bigquery_api, self.client.project_id(), job=job)
            if insert_job.wait_for_done():
                result = insert_job.get()

                job = {
                    'projectId': self.client.project_id(),
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
                            'destinationFormat': self.get_service_value("destinationFormat", "NEWLINE_DELIMITED_JSON")
                        }
                    }
                }
                extract_job = _BqJob(self.bigquery_api, self.client.project_id(), job=job)
                if extract_job.wait_for_done():
                    self._success()
                else:
                    extract_job.raise_error("Error extracting query data from Google BigQuery")
            else:
                insert_job.raise_error("Error querying to temp table in Google BigQuery")


class BigQueryViewTask(_BqTask):
    """ Create a new view from a query

        Query can be supplied either by overwriting the query_file method and providing the path to a file containing
        the query OR by overwriting the query method and returning the query as a string.
    """

    def __init__(self, *args, **kwargs):
        super(BigQueryViewTask, self).__init__(*args, **kwargs)

    def output(self):
        return BigQueryTarget(self.table(), None, self.client)

    def query(self):
        with open(self.query_file(), 'r') as f:
            content = f.read()
        return content

    def query_file(self):
        return NotImplementedError

    def table(self):
        """
        Format: projectId:datasetId.tableId
        :return:
        """
        return NotImplementedError

    def run(self):
        query = Template(self.query()).substitute(self.variables())
        parsed_ids = _table_with_default(self.table(), self.client)

        request_body = {
            'tableReference': {
                'projectId': parsed_ids['projectId'],
                'datasetId': parsed_ids['datasetId'],
                'tableId': parsed_ids['tableId']
            },
            'view': {
                'query': query
            }
        }

        insert_response = self.bigquery_api.tables().insert(projectId=parsed_ids['projectId'],
                                                            datasetId=parsed_ids['datasetId'],
                                                            body=request_body).execute()

        if 'error' in insert_response:
            raise Exception("Error in creating view: " + str(insert_response['error']['errors']))
