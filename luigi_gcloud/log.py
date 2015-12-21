import logging

from luigi_gcloud.gcore import get_default_client


class CloudLoggingHandler(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self)
        self.name = "luigi-gcloud"
        client = get_default_client()
        self.project_id = self.project_id = client.project_id()
        http = client.http_authorized()
        self.logging_api = client.logging_api(http)

    def emit(self, record):
        # print(str(vars(record)))
        self.logging_api.entries().write(
                body={
                    "entries": [
                        {
                            "severity": record.levelname,
                            "jsonPayload": {
                                "logger": record.name,
                                "module": record.module,
                                "message": record.getMessage()
                            },
                            "logName": "projects/" + self.project_id + "/logs/" + self.name,
                            "resource": {
                                "type": "global",
                            }
                        }
                    ]
                }
        ).execute()
