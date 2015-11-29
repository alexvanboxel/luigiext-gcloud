import io
import logging

import luigi
from luigi.contrib import gcs

from luigi_gcloud.gcore import get_default_client

logger = logging.getLogger('luigi-gcloud')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")


# noinspection PyAbstractClass
class GCSFileSystem(gcs.GCSClient):
    def touch(self, dest_path):
        media = apiclient.http.MediaIoBaseUpload(io.BytesIO(''), 'application/octet-stream')
        bucket, obj = self._path_to_bucket_and_key(dest_path)

        return self.client.objects().insert(bucket=bucket, name=obj, media_body=media).execute()

    def __init__(self, client=None, descriptor='', http_=None, chunksize=gcs.CHUNKSIZE):
        client = client or get_default_client()
        super(GCSFileSystem, self).__init__(client.oauth(), descriptor, http_, chunksize)


# noinspection PyAbstractClass
class GCSTarget(gcs.GCSTarget):
    storage_api = None

    def bucket(self):
        ix = self.path.find('/', 5)
        return self.path[5:ix]

    def path_in_bucket(self):
        ix = self.path.find('/', 5)
        return self.path[ix + 1:]

    def __repr__(self):
        return self.path

    def __init__(self, path, format=None, client=None):
        client = client or get_default_client()
        self.storage_api = client.storage_api()
        super(GCSTarget, self).__init__(path, format, client=GCSFileSystem(client))

    def touch(self):
        out = self.fs.touch(self.path)
        logger.warning("TODO: Need to handle out: " + str(out))


class GCSFlagTarget(GCSTarget):
    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        if path[-1] != "/":
            path += "/"
        super(GCSFlagTarget, self).__init__(path + flag, format, client)


class AtomicGCSFile(luigi.target.AtomicLocalFile):
    def __init__(self, path, client=None):
        client = client or get_default_client()
        self.gcs_client = gcs.GCSClient(client.oauth())
        super(AtomicGCSFile, self).__init__(path)

    def move_to_final_destination(self):
        self.gcs_client.put(self.tmp_path, self.path)


class MarkerTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        client = kwargs.get("api") or get_default_client()
        self._gcs = client.storage_api()
        super(MarkerTask, self).__init__(*args, **kwargs)

    def run(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        else:
            logger.error("Output " + str(marker) + " not writable")
