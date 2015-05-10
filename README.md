# luigiext-gcloud

Luigi tasks I use to communicate with Google Cloud Platform. For now it has some 
BigQuery tasks. For connection with Cloud Storage we use *gcs-connector* through
Hadoop, it works but could be faster through the native API. For an article about 
the usage goto: 

http://alex.vanboxel.be/2015/03/12/using-luigi-on-the-google-cloud-platform/


Example of config (local auth with secret):

 $ cat /etc/luigi/client.cfg

```
[gcloud]
auth=secret
auth.secret.file=/Users/alexvanboxel/flow/testsecret.json
auth.credentials.file=/Users/alexvanboxel/flow/credentials.json

project.number=0000000000000
project.id=my-project-name

dataflow.staging=gs://bucket/staging/
dataflow.java.path=java
dataflow.runner=BlockingDataflowPipelineRunner
dataflow.zone=europe-west1-d
```

Example of config (in cloud service account):

 $ cat /etc/luigi/client.cfg

```
[gcloud]
auth=service

project.number=0000000000000
project.id=my-project-name

dataflow.staging=gs://bucket/staging/
dataflow.java.path=java
dataflow.runner=BlockingDataflowPipelineRunner
dataflow.zone=europe-west1-d
```

## Google Cloud Storage

Example task using the GCSTarget:

```python
from luigiext.gcs import GCSTarget

class MyTask(luigi.Task):

    def output(self):
        return GCSTarget(self.day.strftime('gs://bucket/data/aggregate/daily/foobar/%Y/%m/%d/'))
```

Example task using the GCSFlagTarget: 

```python
from luigiext import gcs
from luigiext.gcs import GCSFlagTarget

class NightlyTask(gcs.MarkerTask):
    day = luigi.DateParameter

    def output(self):
        return GCSFlagTarget(self.day.strftime('gs://bucket/data/marker/foobar/%Y/%m/%d/'), flag='_RUN')

```

## Google Big Query

Task using the BigQueryTask. Look to the existence of a BQ table.

```python
class MyTask(luigi.Task):

    def table(self):
        return self.day.strftime("0000000000000:foo.bar")

    def output(self):
        return bigquery.BigQueryTarget(
            table=self.table()
        )
```

Task using the BigQueryTask. Look to the existence of a a range in the table (result of query).

```python
class MyTask(luigi.Task):

    def table(self):
        return self.day.strftime("0000000000000:foo.bar")

    def output(self):
        return bigquery.BigQueryTarget(
            table=self.table(),
            query=self.day.strftime("SELECT 0 < count(bucket_date) "
                                    "FROM [foo.bar] "
                                    "WHERE bucket_date = '%Y-%m-%d 00:00:00 UTC'"),
        )
```

Task that loads data from storage to a BigQuery table:

```python
class MyMailLoadTask(bigquery.BqTableLoadTask):

    def schema(self):
        return [
            {"name": "datetime", "type": "timestamp", "mode": "required"},
            {"name": "name", "type": "string", "mode": "required"},
            {"name": "id", "type": "integer", "mode": "nullable"},
            {"name": "mailing", "type": "string", "mode": "nullable"}
        ]

    def source(self):
        return 'gs://bucket/data/output/bigquery/mail/part*'

    def table(self):
        return "0000000000000:foo.bar"

    def output(self):
        return GCSFlagTarget('/bucket/data/output/bigquery/mail/', flag='_EXPORTED')
```

Task that executes BQ query and output a new table:

```python
class MyQueryToTableTask(bigquery.BqQueryTask):
    day = luigi.DateParameter(default=dateutils.yester_day())

    def table(self):
        return self.day.strftime("0000000000000:foo.bar")

    def query(self):
        return load("queries/query_file.bq")

    def output(self):
        return GCSFlagTarget(self.day.strftime('/data/aggregate/marker/mail/%Y/%m/%d/'),
                             flag='_MAILED')

    def configuration(self):
        return {
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND",
            "allowLargeResults": True
        }

    def schema(self):
        return None

```

Task that executes BQ query and outputs to storage:

```python
class MyQueryToStorageTask(bigquery.BqQueryTask):
    day = datetime.date.today()

    def output(self):
        return GCSTarget(self.day.strftime('gs://bucket/data/output/%Y/%m/%d/'))

    def destination(self):
        return self.day.strftime('gs://bucket/data/output/%Y/%m/%d/part-r-*.avro')

    def configuration(self):
        return {
            "writeDisposition": "WRITE_TRUNCATE"
        }

    def query(self):
        return load("queries/query_file.bq")

```



## Google Cloud DataFlow

Task that executes BQ query and output a new table:

```python
class MyDataFlowToBigQueryTask(dataflow.DataFlowJavaTask):
    day = luigi.DateParameter

    def dataflow(self):
        return "/pipeline/flow/build/libs/pipeline/flow-all-1.0.jar"

    def table(self):
        return self.day.strftime("0000000000000:foo.bar")
 
    def configuration(self):
        return {
            "basePath": "/workflow/dataflow",
            "autoscalingAlgorithm": "BASIC",
            "maxNumWorkers": "50"
        }

    def params(self):
        return {
            'inputBase': 'gs://bucket/data/someinput/',
            'month': self.day.strftime('%Y-%m')
        }

    def output(self):
        return bigquery.BigQueryTarget(
            table=self.table()
        )
```

