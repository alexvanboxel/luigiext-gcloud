import io

import luigi

from luigi_gcloud.gcore import get_default_api

__author__ = 'alexvanboxel'

import logging

# from luigi import six

from googleapiclient.errors import HttpError
from luigi.target import FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile


logger = logging.getLogger('luigi-gcloud')

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


class GCSFileSystem(FileSystem):
    """
    Google Cloud Storage implementation backed by the Google API.
    """

    def __init__(self, api=None):
        api = api or get_default_api()
        self._gcs = api.storage_api()
        self._bucket = api.bucket()

    def storage_api(self):
        return self._gcs

    def bucket(self):
        return self._bucket

    def remove(self, path, recursive=True, skip_trash=True):
        pass

    def exists(self, path):
        if path[-1:] == '/':
            l = self._gcs.objects().list(bucket=self._bucket,
                                         maxResults=5,
                                         prefix=path)
            result = l.execute()
            if "items" in result and len(result["items"]) > 0:
                return True
            return False
        else:
            try:
                o = self._gcs.objects().get(bucket=self._bucket,
                                            object=path)
                o.execute()
                logger.debug("GCS found gs://"+self._bucket + "/" + path)
                return True
            except HttpError:
                logger.debug("GCS NOT FOUND gs://"+self._bucket + "/" + path)
                return False

    def isdir(self, path):
        pass

    def mkdir(self, path, parents=True, raise_if_exists=False):
        pass


class GCSTarget(FileSystemTarget):
    format = None
    fs = None
    _bucket = None
    _gcs = None

    def __repr__(self):
        return "gs://" + self._bucket + "/" + self.path

    def __init__(self, full_path, format=luigi.format.get_default_format(), fs=None):
        self.fs = fs or GCSFileSystem()
        self._gcs = self.fs.storage_api()
        self.format = format
        if full_path.startswith('gs://'):
            ix = full_path.find('/', 5)
            self._bucket = full_path[5:ix]
            path = full_path[ix + 1:]
        elif full_path.startswith('/'):
            self._bucket = self.fs.bucket()
            path = full_path[1:]
        else:
            self._bucket = self.fs.bucket()
            path = full_path

        super(GCSTarget, self).__init__(path)

    def touch(self):
        media = apiclient.http.MediaIoBaseUpload(io.BytesIO(''), 'application/octet-stream')
        req = self._gcs.objects().insert(
            bucket=self._bucket,
            name=self.path,
            media_body=media)
        out = req.execute()
        logger.warning("TODO: Need to handle out: "+str(out))

    def open(self, mode='r'):
        if mode == 'r':
            return self.format.pipe_reader(self.fs.download(self.path))
        elif mode == 'w':
            print(self._bucket + "==="+self.path+"==="+str(self._gcs))
            return self.format.pipe_writer(AtomicGCSFile(self._bucket, self.path, self._gcs))
        else:
            raise ValueError("Unsupported open mode '{}'".format(mode))


class GCSFlagTarget(GCSTarget):
    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        if path[-1] != "/":
            path += "/"
        super(GCSFlagTarget, self).__init__(path + flag, format, client)


class AtomicGCSFile(AtomicLocalFile):
    def __init__(self, bucket, path, storage_api):
        self._gcs = storage_api
        self._bucket = bucket
        super(AtomicGCSFile, self).__init__(path)

    def move_to_final_destination(self):
        media = apiclient.http.MediaFileUpload(self.tmp_path, 'application/octet-stream')
        req = self._gcs.objects().insert(
            bucket=self._bucket,
            name=self.path,
            media_body=media)
        out = req.execute()
        logger.warning("TODO: Need to handle out: "+str(out))


class MarkerTask(luigi.Task):
    def __init__(self, *args, **kwargs):
        api = kwargs.get("api") or get_default_api()
        self._gcs = api.storage_api()
        super(MarkerTask, self).__init__(*args, **kwargs)

    def run(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        else:
            logger.error("Output " + str(marker) + " not writable")
