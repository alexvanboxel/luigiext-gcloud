import datetime

import luigi

import target
from luigi_gcloud.storage import GCSFileSystem


class CopyLocalToStorage(luigi.Task):
    day = luigi.DateParameter()

    def output(self):
        return target.storage_mail_path(self.day).gcs

    def run(self):
        fs = GCSFileSystem()
        fs.put(target.local_mail_path(self.day),
               target.storage_mail_path(self.day).path)


class CopyAllLocalToStorage(luigi.WrapperTask):
    def requires(self):
        return [
            CopyLocalToStorage(datetime.date(2015, 11, 23)),
            CopyLocalToStorage(datetime.date(2015, 11, 24)),
            CopyLocalToStorage(datetime.date(2015, 11, 25)),
            CopyLocalToStorage(datetime.date(2015, 11, 26)),
            CopyLocalToStorage(datetime.date(2015, 11, 27)),
            CopyLocalToStorage(datetime.date(2015, 11, 28)),
            CopyLocalToStorage(datetime.date(2015, 11, 29))
        ]
