import datetime
import logging

import luigi
from luigi.interface import setup_interface_logging

from bigquery_tasks import CopyBigQueryToStorage
from luigi_gcloud import gcore
from dataflow_tasks import CopyViaDataFlowToStorage
from dataproc_tasks import DataProcPigCopy, DataProcSparkCopy
from luigi_gcloud.gcore import load_default_client, GCloudClient
from storage_tasks import CopyAllLocalToStorage

logger = logging.getLogger('luigi-interface')

gclient = None


def google_default_api():
    global gclient
    if gclient is None:
        gclient = GCloudClient()
        gcore.set_default_client(gclient)


class AllExamples(luigi.WrapperTask):
    def requires(self):
        return [
            CopyAllLocalToStorage(),
            DataProcExamples(),
            CopyViaDataFlowToStorage(datetime.date(2015, 11, 23)),
            CopyBigQueryToStorage(datetime.date(2015, 11, 24))
        ]


class DataProcExamples(luigi.WrapperTask):
    def requires(self):
        return [
            DataProcPigCopy(datetime.date(2015, 11, 23)),
            DataProcSparkCopy(datetime.date(2015, 11, 24)),
        ]


if __name__ == "__main__":
    load_default_client("examples", "examples")
    setup_interface_logging('examples/logging.ini')
    luigi.run()
