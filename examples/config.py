from luigi_gcloud import gcore
from luigi_gcloud.gcore import GCloudClient

__author__ = 'alexvanboxel'

gclient = None


def google_default_api():
    global gclient
    if gclient is None:
        gclient = GCloudClient()
        gcore.set_default_client(gclient)
