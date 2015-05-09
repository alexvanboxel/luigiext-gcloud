import io

from luigiext.gcore import GCloudClient


__author__ = 'alexvanboxel'

import logging

# from luigi import six

from googleapiclient.errors import HttpError
from luigi.target import FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile


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


class GCSFileSystem(FileSystem):
    """
    Google Cloud Storage implementation backed by the Google API.
    """

    _api = None
    _bucket = None


    def api(self):
        return self._api

    def bucket(self):
        return self._bucket

    def remove(self, path, recursive=True, skip_trash=True):
        pass

    def exists(self, path):
        if path[-1:] == '/':
            l = self._api.storage_api().objects().list(bucket=self._bucket,
                                                   maxResults=5,
                                                   prefix=path)
            result = l.execute()
            if result.has_key("items") and len(result["items"]) > 0:
                return True
            return False
        else:
            try:
                o = self._api.storage_api().objects().get(bucket=self._bucket,
                                                      object=path)
                result = o.execute()
                print(result)
                return True
            except HttpError:
                print(path + "NOT FOUND")
                return False

    def isdir(self, path):
        pass

    def mkdir(self, path, parents=True, raise_if_exists=False):
        pass

    def __init__(self, api=None,
                 **kwargs):
        self._api = api or GCloudClient()
        self._bucket = self._api.bucket()
        print "cc"


class GCSTarget(FileSystemTarget):
    fs = None
    bucket = None
    api = None

    def __repr__(self):
        return "gs://" + self.bucket + "/" + self.path

    def __init__(self, full_path, format=None, fs=None):
        self.fs = fs or GCSFileSystem()
        self.api = self.fs.api()
        if full_path.startswith('gs://'):
            ix = full_path.find('/', 5)
            self.bucket = full_path[5:ix]
            path = full_path[ix + 1:]
        elif full_path.startswith('/'):
            self.bucket = self.fs.bucket()
            path = full_path[1:]
        else:
            self.bucket = self.fs.bucket()
            path = full_path

        super(GCSTarget, self).__init__(path)

    def touch(self):
        media = apiclient.http.MediaIoBaseUpload(io.BytesIO(''), 'application/octet-stream')
        req = self.api.storage_api().objects().insert(
            bucket=self.bucket,
            name=self.path,
            media_body=media)
        out = req.execute()
        print(out)

    def open(self, mode):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            s3_key = self.fs.get_key(self.path)
            if not s3_key:
                raise FileNotFoundException("Could not find file at %s" % self.path)

            fileobj = ReadableGCSFile(self._api)
            return self.format.pipe_reader(fileobj)
        else:
            return self.format.pipe_writer(AtomicGCSFile(self.path, self.fs.api()))


class GCSFlagTarget(GCSTarget):
    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        if path[-1] != "/":
            path += "/"
        super(GCSFlagTarget, self).__init__(path+flag, format, client)


class ReadableGCSFile():
    def sss(self):
        print()


class AtomicGCSFile(AtomicLocalFile):
    """
    An file that writes to a temp file and put to S3 on close.
    """
    bucket = None

    def __init__(self, bucket, path, api):
        self._api = api
        self.bucket = bucket
        super(AtomicGCSFile, self).__init__(path)

    def move_to_final_destination(self):
        media = apiclient.http.MediaFileUpload(self.tmp_path(), 'application/octet-stream')
        req = self._api.storage_api().objects().insert(
            bucket=self.bucket,
            name=self.path,
            media_body=media)
        out = req.execute()




