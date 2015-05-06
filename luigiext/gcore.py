from ConfigParser import NoSectionError
import webbrowser

import logging
import os
import os.path
import luigi

from luigi import configuration
# from luigi import six

from luigi.format import FileWrapper
from luigi.parameter import Parameter
from luigi.target import FileSystem, FileSystemException, FileSystemTarget
from luigi.task import ExternalTask
import sys
from googleapiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError, OAuth2Credentials
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run
from oauth2client.tools import client as oauthclient
from googleapiclient.errors import HttpError
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


def load(file_name):
    with open(file_name, 'r') as f:
        content=f.read()
    return content


class GCloudClient:

    def __init__(self, **kwargs):
        # for key, value in kwargs.iteritems():
        #     print "%s = %s" % (key, value)

        scope = kwargs.get("scope") or [
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/bigquery'
        ]

        config = dict(configuration.get_config().items('gcloud'))
        print(config)
        auth_method = config.get("auth", "service")
        if auth_method == 'secret':
            secret_file = config.get("auth.secret.file", "secret.json")
            credentials_file = config.get("auth.credentials.file", "credentials.json")
            flow = oauthclient.flow_from_clientsecrets(
                secret_file,
                scope=scope,
                redirect_uri='urn:ietf:wg:oauth:2.0:oob')
            try:
                token_file = open(credentials_file, 'r')
                self.credentials = OAuth2Credentials.from_json(token_file.read())
                token_file.close();
            except IOError:
                auth_uri = flow.step1_get_authorize_url()
                webbrowser.open(auth_uri)
                auth_code = raw_input('Enter the auth code: ')

                self.credentials = flow.step2_exchange(auth_code)
                token_file = open(credentials_file, 'w')
                token_file.write(self.credentials.to_json());
                token_file.close();
        elif auth_method == 'service':
            self.credentials = gce.AppAssertionCredentials(scope=scope)

        self._http = self.credentials.authorize(httplib2.Http())
        self._project_id = config.get("project_id")

    def http(self):
        return self._http

    def bucket(self):
        return "vex-eu-data"

    def bigquery(self):
        return build('bigquery', 'v2', http=self._http)

    def storage(self):
        return build('storage', 'v1', http=self._http)

    def project_id(self):
        return self._project_id