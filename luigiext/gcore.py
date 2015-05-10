import webbrowser
import logging

from luigi import configuration



# from luigi import six

from googleapiclient.discovery import build
from oauth2client.client import OAuth2Credentials
from oauth2client.tools import client as oauthclient
import httplib2
from oauth2client import gce


logger = logging.getLogger('luigi-interface')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")


def load(file_name):
    with open(file_name, 'r') as f:
        content = f.read()
    return content


class GCloudClient:
    def __init__(self, **kwargs):
        # for key, value in kwargs.iteritems():
        #     print "%s = %s" % (key, value)

        scope = kwargs.get("scope") or [
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/bigquery'
        ]

        self.config = dict(configuration.get_config().items('gcloud'))
        print(self.config)
        auth_method = self.config.get("auth", "service")
        if auth_method == 'secret':
            secret_file = self.config.get("auth.secret.file", "secret.json")
            credentials_file = self.config.get("auth.credentials.file", "credentials.json")
            flow = oauthclient.flow_from_clientsecrets(
                secret_file,
                scope=scope,
                redirect_uri='urn:ietf:wg:oauth:2.0:oob')
            try:
                token_file = open(credentials_file, 'r')
                self.credentials = OAuth2Credentials.from_json(token_file.read())
                token_file.close()
            except IOError:
                auth_uri = flow.step1_get_authorize_url()
                webbrowser.open(auth_uri)
                auth_code = raw_input('Enter the auth code: ')

                self.credentials = flow.step2_exchange(auth_code)
                token_file = open(credentials_file, 'w')
                token_file.write(self.credentials.to_json())
                token_file.close()
        elif auth_method == 'service':
            self.credentials = gce.AppAssertionCredentials(scope=scope)

        self._project_number = self.config.get("project.number")
        self._project_id = self.config.get("project.id")

        self._defaults = {

        }

    def http(self):
        return self.credentials.authorize(httplib2.Http())

    def bucket(self):
        return "vex-eu-data"

    def bigquery_api(self, http=None):
        return build('bigquery', 'v2', http=http or self.http())

    def storage_api(self, http=None):
        return build('storage', 'v1', http=http or self.http())

    def dataflow_api(self, http=None):
        return build('dataflow', 'v1b3', http=http or self.http())

    def project_number(self):
        return self._project_number

    def project_id(self):
        return self._project_id

    def get(self, name):
        return self.config.get(name) or self._defaults.get(name)


default_client = None


def set_default_api(gclient):
    global default_client
    default_client = gclient


def get_default_api():
    global default_client
    if default_client is None:
        default_client = GCloudClient()
    return default_client
