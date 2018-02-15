# Write billing items to S3

import boto3
import json
import logging
import os

ARCHIVE_S3_BUCKET_NAME = os.environ.get('ARCHIVE_S3_BUCKET_NAME')

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))
_logger = logging.getLogger(__name__)


s3_client = boto3.client('s3')

def _get_line_item_from_event(event):
    '''Return the line item from the event.'''
    sns_message = event.get('Records')[0].get('Sns').get('Message')
    line_item = json.loads(sns_message)

    return line_item


def _get_s3_key(line_item):
    item_id = line_item.get('identity').get('LineItemId')

    return item_id + '.json'


def _write_item_to_s3(s3_bucket, s3_key, line_item):
    resp = s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(line_item)
    )

    return resp


def handler(event, context):
    _logger.info('Event received: {}'.format(json.dumps(event)))
    line_item = _get_line_item_from_event(event)
    s3_key = _get_s3_key(line_item)
    s3_resp = _write_item_to_s3(ARCHIVE_S3_BUCKET_NAME, s3_key, line_item)

    resp = {
        's3': s3_resp
    }

    _logger.info('Response: {}'.format(json.dumps(resp)))

    return resp

