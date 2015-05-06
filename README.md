# luigiext-gcloud

Luigi tasks I use to communicate with Google Cloud Platform. For now it has some 
BigQuery tasks. For connection with Cloud Storage we use *gcs-connector* through
Hadoop, it works but could be faster through the native API. For an article about 
the usage goto: 

http://alex.vanboxel.be/2015/03/12/using-luigi-on-the-google-cloud-platform/


Example of config (local auth with secret):

 $ cat /etc/luigi/client.cfg

[gcloud]
auth=secret
auth.secret.file=/Users/alexvanboxel/flow/testsecret.json
auth.credentials.file=/Users/alexvanboxel/flow/credentials.json
project_id=0000000000000


Example of config (in cloud service account):

 $ cat /etc/luigi/client.cfg

[gcloud]
auth=service
project_id=0000000000000
