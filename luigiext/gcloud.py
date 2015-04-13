from ConfigParser import NoSectionError


import logging
import os
import os.path
import luigi

from luigi import configuration
#from luigi import six

from luigi.format import FileWrapper
from luigi.parameter import Parameter
from luigi.target import FileSystem, FileSystemException, FileSystemTarget
from luigi.task import ExternalTask
import sys
from apiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run
from apiclient.errors import HttpError
import httplib2
from oauth2client import gce
import time
import io

logger = logging.getLogger('luigi-interface')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")



class InvalidDeleteException(FileSystemException):
    pass


class FileNotFoundException(FileSystemException):
    pass


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



class BqTableTarget(luigi.Target):

    def __init__(self, table, client=None):
        self.credentials = gce.AppAssertionCredentials(scope='https://www.googleapis.com/auth/devstorage.read_write')
        self.http = self.credentials.authorize(httplib2.Http())
        self.service = build('bigquery', 'v2', http=self.http)

        self.table = _split_tablename(table)

    def exists(self):
        print "checking existence"
        tables = self.service.tables()
        print "tables"
        try:
            table = tables.get(projectId=self.table['projectId'], datasetId=self.table['datasetId'], tableId=self.table['tableId']).execute()
            print "table"
            logger.debug(table)
            return True
        except HttpError as err:
            print "ERROR"
            return False


class BqQueryTarget(luigi.Target):

    def __init__(self, table, query, client=None):
        self.credentials = gce.AppAssertionCredentials(scope='https://www.googleapis.com/auth/devstorage.read_write')
        self.http = self.credentials.authorize(httplib2.Http())
        self.service = build('bigquery', 'v2', http=self.http)
        self.table = _split_tablename(table)
        print "TABLE SPLIT: " + str(self.table)
        self.query = query

    def query(self):
        raise NotImplementedError

    def exists(self):
        print "checking existence"
        tables = self.service.tables()
        print "tables"
        try:
            print "TABLE GET : " + str(self.table)
            table = tables.get(projectId=self.table['projectId'], datasetId=self.table['datasetId'], tableId=self.table['tableId']).execute()
            print "table"
            jobs = self.service.jobs()
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
            insertJob = jobs.insert(projectId=self.table['projectId'], body=job).execute()
            while True:
                status =jobs.get(projectId=self.table['projectId'], jobId=insertJob['jobReference']['jobId']).execute()
                print status
                if 'DONE' == status['status']['state']:
                    print "Done exporting!" + str(status)
                    result = jobs.getQueryResults(projectId=self.table['projectId'],jobId=insertJob['jobReference']['jobId']).execute();
                    print "RESULT : " + str(result)
                    print "VALUE : " + str(result['rows'][0]['f'][0]['v'])
                    return result['rows'][0]['f'][0]['v'] in ['true', 'True']
                print 'Waiting for export to complete..'
                time.sleep(5)
            return False
        except HttpError as err:
            print "ERROR"
            return False


class BqTableExtractTask(luigi.Task):
    def run(self):
        logger.info("Running BqTableExtractTask")


class BqTableLoadTask(luigi.Task):
    """ Load/Append data into a BigQuery table """

    def __init__(self, *args, **kwargs):
        self.credentials = gce.AppAssertionCredentials(scope=[
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/devstorage.read_write'
        ])
        self.http = self.credentials.authorize(httplib2.Http())
        self.bq = build('bigquery', 'v2', http=self.http)
        self.gcs = build('storage', 'v1', http=self.http)
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

    def output_marker(self):
        return {}

    # def configure_source_format(self):
    #     """Source Format default CSV. Possible values CSV,DATASTORE_BACKUP,NEWLINE_DELIMITED_JSON"""
    #     return "CSV"

    def run(self):
        table = _split_tablename(self.table())
        jobs = self.bq.jobs()
        job = {
            'projectId': table['projectId'],
            'configuration': {
                'load': {
                    #'quote':'',
                    'destinationTable': {
                        'projectId': table['projectId'],
                        'datasetId': table['datasetId'],
                        'tableId': table['tableId']
                    },
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON',
                    'writeDisposition': 'WRITE_APPEND',
                    'sourceUris': [ self.source() ],
                    'maxBadRecords': 1000,
                    'schema': {
                        'fields':self.schema()
                    }
                }
            }
        }
        print "JOB : " + str(job)
        insertJob = jobs.insert(projectId=table['projectId'], body=job).execute()
        while True:
            status =jobs.get(projectId=table['projectId'], jobId=insertJob['jobReference']['jobId']).execute()
            print status
            if 'DONE' == status['status']['state']:
                print "Done exporting!" + str(status)
                marker = self.output_marker()
                if 'bucket' in marker:
                    media = apiclient.http.MediaIoBaseUpload(io.BytesIO(''), 'application/octet-stream')
                    req = self.gcs.objects().insert(
                        bucket=marker['bucket'],
                        name=marker['name'],
                        # body=object_resource,
                        media_body=media)
                    out = req.execute()
                    print "MARKER " + str(out)
                return
            print 'Waiting for export to complete..'
            time.sleep(10)
        return



        logger.info("Running BqTableLoadTask")


class BqTableCopyTask(luigi.Task):
    def run(self):
        logger.info("Running BqTableCopyTask")


class StorageMarkerTask(luigi.Task):

    def __init__(self, *args, **kwargs):
        self.credentials = gce.AppAssertionCredentials(scope=[
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/devstorage.read_write'
        ])
        self.http = self.credentials.authorize(httplib2.Http())
        self.gcs = build('storage', 'v1', http=self.http)
        super(StorageMarkerTask, self).__init__(*args, **kwargs)

    def output_marker(self):
        return {}

    def output(self):
        marker = self.output_marker()
        if 'bucket' in marker:
            return luigi.hdfs.HdfsTarget(
                self.day.strftime('/'+marker['name'])
            )

    def run(self):
        marker = self.output_marker()
        if 'bucket' in marker:
            media = apiclient.http.MediaIoBaseUpload(io.BytesIO(''), 'application/octet-stream')
            req = self.gcs.objects().insert(
                bucket=marker['bucket'],
                name=marker['name'],
                # body=object_resource,
                media_body=media)
            out = req.execute()
            print "MARKER " + str(out)
