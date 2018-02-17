'''Fetch object from S3 and publish items to SNS'''

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

def _convert_empty_value_to_none(item):
    '''Turn empty strings into None, etc.'''

    # DynamoDB can't have empty strings but csv.DictReader earlier in system
    # uses '' for empty fields.
    for key, value in item.items():
        if value == '':
            item[key] = None

    return item


def _delete_s3_object(s3_bucket, s3_key):
    '''Get object body from S3.'''
    resp = s3_client.delete_object(
        Bucket=s3_bucket,
        Key=s3_key
    )

    return resp


def _publish_sns_message(topic_arn, line_item):
    '''Publish message to SNS'''
    resp = sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(line_item)
    )

    return resp


def _get_s3_object_body(s3_bucket, s3_key):
    '''Get object body from S3.'''
    s3_object = s3_client.get_object(
        Bucket=s3_bucket,
        Key=s3_key
    )

    s3_object_body = s3_object.get('Body').read().decode()

    return s3_object_body


def _create_line_item_message(headers, line_item):
    '''Return a formatted line item message.'''
    split_line_item = line_item.split(',')
    item_dict = dict(zip(headers, split_line_item))
    sanitized_item_dict = _convert_empty_value_to_none(item_dict)

    final_dict = _format_linet_item_dict(sanitized_item_dict)

    return final_dict


def _format_linet_item_dict(line_item_dict):
    '''Convert multi-level keys into parent/child dict values.'''
    formatted_line_item_dict = {}

    for k, v in line_item_dict.items():
        key_list = k.split('/')
        if len(key_list) > 1:
            parent, child = key_list
            if parent not in formatted_line_item_dict.keys():
                formatted_line_item_dict[parent] = {}
            formatted_line_item_dict[parent][child] = v
        else:
            formatted_line_item_dict[key_list[0]] = v

    return formatted_line_item_dict


def _process_additional_items(arn, event, record_offset):
    '''Process additional records.'''
    resp = event.get('Records')[0][X_RECORD_OFFSET] = record_offset
    lambda_client.invoke(
        FunctionName=arn,
        Payload=json.dumps(event),
        InvocationType='Event'
    )

    return resp


def handler(event, context):
    _logger.info('S3 event received: {}'.format(json.dumps(event)))
    s3_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
    s3_key = event.get('Records')[0].get('s3').get('object').get('key')
    record_offset=  event.get('Records')[0].get(X_RECORD_OFFSET, 0)

    s3_object_body = _get_s3_object_body(s3_bucket, s3_key)
    s3_body_file = io.StringIO(s3_object_body)

    # Get header so we can format messages.
    record_headers = s3_body_file.readline().strip().split(',')

    sns_resp = []

    # FIXME: This block has caused us to need to allocate more memory. We
    # should get more efficient with this.
    total_line_items = s3_body_file.readlines()
    len_total_line_items = len(total_line_items)
    _logger.info('Total items: {}'.format(len_total_line_items))
    line_items = total_line_items[record_offset:]

    # NOTE: We might decide to batch send multiple records at a time.  It's
    # Worth a look after we have decne t metrics to understand tradeoffs.
    for line_item in line_items:
        record_offset += 1
        _logger.info('Publishing line_item: {}'.format(record_offset))

        stripped_line_item = line_item.strip()

        line_item_msg = _create_line_item_message(record_headers, stripped_line_item)
        _logger.debug('message: {}'.format(json.dumps(line_item_msg)))

        resp = _publish_sns_message(AWS_SNS_TOPIC, line_item_msg)
        _logger.debug(
            'Publish response for line_item {}: {}'.format(
                record_offset, json.dumps(resp)
            )
        )

        sns_resp.append(resp)

        if context.get_remaining_time_in_millis() <= 2000:
            break

    # We're done.  Remove file.
    if record_offset < len_total_line_items:
        _logger.info('Invoking additional execution at record offset: {}'.format(record_offset))
        lambda_resp = _process_additional_items(context.invoked_function_arn, event, record_offset)
        _logger.info('Invoked additional Lambda response: {}'.format(json.dumps(lambda_resp)))

        s3_response = None
    else:
        s3_response = _delete_s3_object(s3_bucket, s3_key)
        lambda_resp = None

    resp = {
        'sns': sns_resp,
        'lambda': lambda_resp,
        's3': s3_response,
    }

    _logger.info('AWS responses: {}'.format(json.dumps(resp)))
    return resp


