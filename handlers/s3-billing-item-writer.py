# Write billing items to S3

import boto3
import iso8601
import json
import logging
import os

ARCHIVE_S3_BUCKET_NAME = os.environ.get('ARCHIVE_S3_BUCKET_NAME')
S3_PREFIX='aws-adm'

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
    '''Get S3 key based on line item info.'''
    start_time, end_time = line_item.get('identity').get('TimeInterval').split('/')
    item_id = line_item.get('identity').get('LineItemId')
    start_date_time = iso8601.parse_date(start_time)

    s3_key = '{prefix}/year={year}/month={month}/day={day}/{item_id}.json'.format(
        prefix=S3_PREFIX,
        year=start_date_time.year,
        month=start_date_time.month,
        day=start_date_time.day,
        item_id=item_id
    )

    return s3_key


def _write_item_to_s3(s3_bucket, s3_key, line_item):
    '''Write item to S3'''
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

    _logger.info(
        'Writing Data to S3: {}'.format(
            json.dumps(
                {
                    's3_bucket':ARCHIVE_S3_BUCKET_NAME,
                    's3_key':s3_key
                }
            )
        )
    )

    _logger.debug('line_item: {}'.format(json.dumps(line_item)))
    s3_resp = _write_item_to_s3(ARCHIVE_S3_BUCKET_NAME, s3_key, line_item)

    resp = {
        's3': s3_resp
    }

    _logger.info('Response: {}'.format(json.dumps(resp)))

    return resp

