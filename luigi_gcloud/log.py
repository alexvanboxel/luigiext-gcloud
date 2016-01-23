import Queue
import logging
import thread

from luigi_gcloud.gcore import get_default_client


def logger(logging_api, queue):
    while True:
        entry = queue.get()
        logging_api.entries().write(
                body={
                    "entries": [
                        entry
                    ]
                }
        ).execute()
        queue.task_done()


class CloudLoggingHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        self.name = "luigi-gcloud"
        client = get_default_client()
        self.project_id = self.project_id = client.project_id()
        http = client.http_authorized()
        self.logging_api = client.logging_api(http)
        self.queue = Queue.Queue()
        thread.start_new_thread(logger, (self.logging_api, self.queue))

    def emit(self, record):
        # print(str(vars(record)))
        self.queue.put({
            "severity": record.levelname,
            "jsonPayload": {
                "logger": record.name,
                "module": record.module,
                "message": record.getMessage()
            },
            "logName": "projects/" + self.project_id + "/logs/" + self.name,
            "resource": {
                "type": "global",
            }})
