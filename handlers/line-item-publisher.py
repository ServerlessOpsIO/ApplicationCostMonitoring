# Fetch object from S3 and publish items to SNS

import boto3
import io
import json
import logging
import os

AWS_SNS_TOPIC = os.environ.get('AWS_SNS_TOPIC')

X_RECORD_OFFSET = 'x-record-offset'

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))
_logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
lambda_client = boto3.client('lambda')

def _publish_sns_message(topic_arn, message):
    '''Publish message to SNS'''
    resp = sns_client.publish(
        TopicArn=topic_arn,
        Message=message
    )

    return resp


def _get_s3_object(s3_bucket, s3_key):
    '''Get object from S3.'''
    s3_object = s3_client.get_object(
        Bucket=s3_bucket,
        Key = s3_key
    )

    return s3_object


def _process_additional_items(arn, event, record_offset):
    '''Process additional records.'''
    resp = event.get('Records')[0][X_RECORD_OFFSET] = record_offset
    lambda_client.invoke(
        FunctionName=arn,
        Payload=json.dumps(event)
    )

    return resp

def handler(event, context):
    _logger.info('S3 event received: {}'.format(json.dumps(event)))
    s3_bucket_name = event.get('Records')[0].get('s3').get('bucket').get('name')
    s3_object_name = event.get('Records')[0].get('s3').get('object').get('key')
    record_offset=  event.get('Records')[0].get(X_RECORD_OFFSET, 0)

    s3_object = _get_s3_object(s3_bucket_name, s3_object_name)
    s3_body_file = io.StringIO(s3_object.get('Body').read().decode())

    # skip header line.
    if record_offset == 0:
        s3_body_file.readline()
        record_offset += 1

    sns_resp = []
    lambda_resp = None

    line_items = s3_body_file.readlines()[record_offset:]
    len_line_items = len(line_items)

    # NOTE: We c
    for line_item in line_items:
        _logger.info('Publishing line_item: {}'.format(record_offset))
        _logger.debug('line_item data: {}'.format(line_item))
        resp = _publish_sns_message(AWS_SNS_TOPIC, line_item)
        _logger.debug(
            'Publish response for line_item {}: {}'.format(
                record_offset, json.dumps(resp)
            )
        )

        sns_resp.append(resp)
        record_offset += 1

        if context.get_remaining_time_in_millis() <= 2000 and record_offset < len_line_items:
            _logger.info('Invoking additional execution at record offset: {}'.format(record_offset))
            lambbda_resp = _process_additional_items(context.invoked_function_arn, event, record_offset)
            _logger.info('Invoked additional Lambda response: {}'.json.dumps(resp))

            break

    resp = {
        'sns': sns_resp,
        'lambda': lambbda_resp
    }

    _logger.info('SNS responses: {}'.format(json.dumps(resp)))
    return sns_publish_responses


