import pandas as pd
import os
import boto3
import datetime

# AWS #
AWS_CREDENTIALS = {
    'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY']
}

bucket = "mon_bucket"
file_prefix = "un_prefix"

s3 = boto3.resource(
        service_name='s3',
        **AWS_CREDENTIALS
    )
s3_client = boto3.client('s3')


bucket = s3.Bucket(bucket)

df = pd.DataFrame()
for obj in bucket.objects.filter(Prefix='file_prefix'):
        obj = s3_client.get_object(Bucket=bucket.name, Key=obj.key)
        df = df.append(pd.read_csv(obj['Body'], compression='gzip', error_bad_lines=False))


