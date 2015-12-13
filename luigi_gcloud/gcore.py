import logging
import uuid
import webbrowser
from string import Template

import httplib2
import luigi
from googleapiclient.discovery import build
from luigi import configuration
from oauth2client import gce
from oauth2client.client import OAuth2Credentials, GoogleCredentials
from oauth2client.tools import client as oauthclient

logger = logging.getLogger('luigi-gcloud')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")


def load_query_file(file_name):
    with open(file_name, 'r') as f:
        content = f.read()
    return content


class GCloudClient:
    def __init__(self, **kwargs):
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
        self._staging = self.config.get("api.project." + self._project_name + ".staging")
        self._zone = self.config.get("api.project." + self._project_name + ".zone")

    def http_authorized(self):
        return self.credentials.authorize(httplib2.Http())

    @staticmethod
    def http():
        return httplib2.Http()

    def oauth(self):
        return self.credentials

    def project_staging(self):
        return self._staging

    def project_zone(self):
        return self._zone

    def bigquery_api(self, http=None):
        return build('bigquery', 'v2', http=http or self.http_authorized())

    def storage_api(self, http=None):
        return build('storage', 'v1', http=http or self.http_authorized())

    def dataflow_api(self, http=None):
        return build('dataflow', 'v1b3', http=http or self.http_authorized())

    def dataproc_api(self, http=None):
        return build('dataproc', 'v1beta1', http=http or self.http_authorized())

    def project_number(self):
        return self._project_number

    def project_name(self):
        return self._project_name

    def config_name(self):
        return self._config_name

    def project_id(self):
        return self._project_id

    def get(self, api, name, config={}, default=None):
        key = (api + ".configuration." + self._config_name + "." + name).lower()
        value = config.get(name) or self.config.get(key)
        if value is None:
            if default is None:
                raise ValueError("Value for " + name + " not found, either in defaults or configuration as key " + key)
            else:
                return default
        return value


default_client = None


class _GCloudTask(luigi.Task):
    client = None
    service_name = None
    bigquery_api = None
    service_name = None
    uuid = str(uuid.uuid1())[:13]
    _resolved_name = None

    def variables(self):
        return None

    def configuration(self):
        return {}

    def name(self):
        return type(self).__name__

    def resolved_name(self):
        if self._resolved_name is None:
            name = self.name()
            if self.variables() is not None:
                name = Template(self.name()).substitute(self.variables())
            self._resolved_name = name + "__" + self.uuid
        return self._resolved_name

    def get_service_value(self, key, default=None):
        config = self.configuration()
        return self.client.get(self.service_name, key, config, default)

    def get_variable_value(self, default=None):
        return self.get_service_value(self.service_name, 'var', default)

    def __init__(self, *args, **kwargs):
        self.client = kwargs.get("client") or get_default_client()
        super(_GCloudTask, self).__init__(*args, **kwargs)


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
