import logging
import webbrowser
# from luigi import six

from googleapiclient.discovery import build
from oauth2client.client import OAuth2Credentials, GoogleCredentials
from oauth2client.tools import client as oauthclient
import httplib2
from oauth2client import gce
from luigi import configuration


logger = logging.getLogger('luigi-gcloud')

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

        self._project_name = kwargs.get("name", "default")
        self._config_name = kwargs.get("config", "default")

        logger.debug(
            "GCloudClient client created client name: " + self._project_name + ", config name: " + self._config_name)
        auth_method = self.config.get("api.project." + self._project_name + ".auth.method", "service")
        if auth_method == 'secret':
            secret_file = self.config.get("api.project." + self._project_name + ".auth.secret.file", "secret.json")
            credentials_file = self.config.get("api.project." + self._project_name + ".auth.credentials.file",
                                               "credentials.json")
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
        elif auth_method == 'default':
            self.credentials = GoogleCredentials.get_application_default()

        self._project_number = self.config.get("api.project." + self._project_name + ".number")
        self._project_id = self.config.get("api.project." + self._project_name + ".id")

        self._defaults = {
            "dataflow.configuration." + self._config_name + ".autoscalingalgorithm": "BASIC",
            "dataflow.configuration." + self._config_name + ".maxnumworkers": "50",
            "dataflow.configuration." + self._config_name + ".basepath": "."
        }

    def http_authorized(self):
        return self.credentials.authorize(httplib2.Http())

    def http(self):
        return httplib2.Http()

    def oauth(self):
        return self.credentials

    def bucket(self):
        return "vex-eu-data"

    def bigquery_api(self, http=None):
        return build('bigquery', 'v2', http=http or self.http_authorized())

    def storage_api(self, http=None):
        return build('storage', 'v1', http=http or self.http_authorized())

    def dataflow_api(self, http=None):
        return build('dataflow', 'v1b3', http=http or self.http_authorized())

    def dataproc_api(self, http=None):
        return build('dataproc', 'v1b3', http=http or self.http_authorized())

    def project_number(self):
        return self._project_number

    def project_name(self):
        return self._project_name

    def config_name(self):
        return self._config_name

    def project_id(self):
        return self._project_id

    def get(self, config, api, name):
        key = (api + ".configuration." + self._config_name + "." + name).lower()
        value = config.get(name) or self.config.get(key) or self._defaults.get(key)
        if value is None:
            raise ValueError("Value for " + name + " not found, either in defaults or configuration as key " + key)
        return value


default_client = None


def set_default_client(gclient):
    global default_client
    default_client = gclient


def load_default_client(name, config):
    global default_client
    default_client = GCloudClient(name=name, config=config)


def get_default_client():
    global default_client
    if default_client is None:
        default_client = GCloudClient()
    return default_client
