from luigi_gcloud.gcs import GCSTarget, GCSFlagTarget

from luigi_gcloud.gcore import get_default_client


class GSStoragePathWrapper:
    def __init__(self, path):
        _client = get_default_client()
        self._bucket = _client.get("var", "bucket")
        self._path = path

    def part(self, part):
        return 'gs://' + self._bucket + '/luigi-gcloud-examples/' + self._path + '/' + part

    @property
    def path(self):
        return 'gs://' + self._bucket + '/luigi-gcloud-examples' + self._path + '/'

    @property
    def gcs(self):
        return GCSTarget('gs://' + self._bucket + '/luigi-gcloud-examples' + self._path + '/')

    @property
    def gcs_flag(self):
        return GCSFlagTarget('gs://' + self._bucket + '/luigi-gcloud-examples' + self._path + '/')


class GSStorageFileWrapper:
    def __init__(self, path):
        _client = get_default_client()
        self._bucket = _client.get("var", "bucket")
        self._path = path

    @property
    def path(self):
        return 'gs://' + self._bucket + '/luigi-gcloud-examples' + self._path

    @property
    def gcs(self):
        return GCSTarget('gs://' + self._bucket + '/luigi-gcloud-examples' + self._path)


def storage_mail_path(bucket):
    return GSStorageFileWrapper(path=bucket.strftime('/mail/%Y/%m/%d/data.txt'))


def storage_mail(bucket, name):
    return GSStoragePathWrapper(path=bucket.strftime('/' + name + '/%Y/%m/%d'))


def local_mail_path(bucket):
    return bucket.strftime('data/mail-%Y-%m-%d.txt')
