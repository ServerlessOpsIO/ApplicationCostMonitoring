# Receive an S3 event, retrieve billing report file, and publish line items to
# SNS topic.

# FIXME: we should at some point potentially breakup decompression from
# parsing, depending on how many invocations it takes to get through a single
# report.

import boto3
import io
import json
import logging
import os
import gzip

from iopipe.iopipe import IOpipe
iopipe = IOpipe()

PROCESSING_S3_BUCKET_NAME = os.environ.get('DECOMPRESS_S3_BUCKET_NAME')

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))
_logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')

def _decompress_s3_object_body(s3_body, filename):
    '''Return the decompressed data of an S3 object.'''
    gzip_file = gzip.GzipFile(fileobj=io.BytesIO(s3_body.read()))
    decompressed_s3_body = gzip_file.read()  # Just need bytes

    return decompressed_s3_body


def _get_s3_object(s3_bucket, s3_key):
    '''return the given S3_object'''
    s3_object = s3_client.get_object(
        Bucket=s3_bucket,
        Key = s3_key
    )

    return s3_object


@iopipe
def handler(event, context):
    _logger.info('S3 event received: {}'.format(json.dumps(event)))
    s3_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
    s3_key = event.get('Records')[0].get('s3').get('object').get('key')

    s3_object = _get_s3_object(s3_bucket, s3_key)
    decompress_s3_key = '.'.join(s3_key.split('.')[:-1])     # drop compression suffix.

    s3_object_data = _decompress_s3_object_body(s3_object.get('Body'), decompress_s3_key)
    resp = s3_client.put_object(
        Bucket=PROCESSING_S3_BUCKET_NAME,
        Key=decompress_s3_key,
        Body=s3_object_data,
    )

    return resp




