# luigiext-gcloud

Luigi tasks I use to communicate with Google Cloud Platform. For now it has some 
BigQuery tasks. For connection with Cloud Storage we use *gcs-connector* through
Hadoop, it works but could be faster through the native API. For an article about 
the usage goto: 

http://alex.vanboxel.be/2015/03/12/using-luigi-on-the-google-cloud-platform/
(deprecated)


## Getting started

Requirements

* Python 2.7
* Java 1.8 (for the DataFlow example)
* Storage bucket
* BigQuery dataset (examples)


```
github 
virtualenv .env
source .env/bin/activate
python setup.py install
```


Example of config (default account):

 $ cat /etc/luigi/client.cfg

```
[gcloud]
api.project.examples.auth.method=default
api.project.examples.number=0000000000000
api.project.examples.id=my-project-name

dataflow.configuration.examples.stagingLocation=gs://my-bucket/staging/
dataflow.configuration.examples.runner=DataflowPipelineRunner
dataflow.configuration.examples.zone=europe-west1-d
dataflow.configuration.examples.basePath=./external/luigi-dataflow
dataflow.configuration.examples.staging=gs://my-bucket/staging/
dataflow.configuration.examples.maxnumworkers=1

var.configuration.examples.bucket=my-bucket

dataflow.java.path=java
```

Example of config (local auth with secret):

 $ cat /etc/luigi/client.cfg

```
[gcloud]
api.project.examples.auth.method=secret
api.project.examples.auth.secret.file=/Users/alexvanboxel/flow/testsecret.json
api.project.examples.auth.credentials.file=/Users/alexvanboxel/flow/credentials.json
api.project.examples.number=0000000000000
api.project.examples.id=my-project-name
```

Example of config (in cloud service account):

 $ cat /etc/luigi/client.cfg

```
[gcloud]
api.project.examples.auth.method=service
api.project.examples.number=0000000000000
api.project.examples.id=my-project-name
```

## Google Cloud Storage

Example task using the GCSTarget:

```python
from luigiext.gcs import GCSTarget

class MyTask(luigi.Task):

    def output(self):
        return GCSTarget(self.day.strftime('gs://bucket/data/aggregate/daily/foobar/%Y/%m/%d/'))
```

Example for a MarkerTask using the GCSFlagTarget: 

```python
from luigiext import gcs
from luigiext.gcs import GCSFlagTarget

class NightlyTask(gcs.MarkerTask):
    day = luigi.DateParameter

    def output(self):
        return GCSFlagTarget(self.day.strftime('gs://bucket/data/marker/foobar/%Y/%m/%d/'), 
            flag='_RUN')

```

Exanple of copying a local file to cloud storage.

```python
class CopyLocalToStorage(luigi.Task):
    day = luigi.DateParameter()

    def output(self):
        return GCSTarget("gs://bucket/data/mail.out")

    def run(self):
        fs = GCSFileSystem()
        fs.put("./data/mail.out", "gs://bucket/data/mail.out")
```

## Google BigQuery


```
    def configuration(self):
        return {
            'sourceFormat': "NEWLINE_DELIMITED_JSON",
            'createDisposition': "CREATE_IF_NEEDED",
            'writeDisposition': "WRITE_APPEND"
        }
```


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
class MyMailLoadTask(bigquery.BigQueryLoadTask):

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

    # Using a flag target to make sure the task only runs once
    def output(self):
        return GCSFlagTarget(self.day.strftime('gs://bucket/data/output/%Y/%m/%d/',flag='_PHASE_03_SQLEXPORT'))

    def destination(self):
        return self.day.strftime('gs://bucket/data/output/%Y/%m/%d/part-r-*.avro')

    # if you set "allowLargeResults" to True, make sure you have a temp DataSet configured
    def configuration(self):
        return {
            "allowLargeResults": False
        }

    def query(self):
        return load("queries/query_file.bq")

```


## Google Cloud DataFlow

Execute a Google Cloud DataFlow.

```python
class CopyViaDataFlowToStorage(DataFlowJavaTask):
    day = luigi.DateParameter()

    def output(self):
        return GCSTarget('gs://bucket/data/out/')

    def requires(self):
        return CopyLocalToStorage(self.day)

    def dataflow(self):
        return "/pipeline/build/libs/pipeline-copy-1.0.jar"

    def configuration(self):
        return {
            "runner": "DataflowPipelineRunner",
            "autoscalingAlgorithm": "BASIC",
            "maxNumWorkers": "80"
        }

    def params(self):
        return {
            'in': 'gs://bucket/data/in/',
            'out': 'gs://bucket/data/out/'
        }
```

