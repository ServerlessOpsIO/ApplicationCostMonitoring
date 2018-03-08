'''Fetch object from S3 and publish items to SNS'''

import boto3
import io
import iso8601
import json
import logging
import os

AWS_SNS_TOPIC = os.environ.get('AWS_SNS_TOPIC')

X_RECORD_OFFSET = 'x-record-offset'
LAST_ADM_ITEM = 'LAST-ADM-ITEM.json'
LINE_ITEM_OFFSET_KEY = 'lineItemOffset'

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))
_logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
lambda_client = boto3.client('lambda')


class BillingReportSchemaChange(Exception):
    pass


def _check_next_record_date(last_line_item, next_line_item):
    # Check that the next record is for a new time period.

    if _get_line_item_time_interval(last_line_item) == _get_line_item_time_interval(next_line_item):
        _logger.error(
            'Next line item is from same time period of last run: {}'.format(
                _get_line_item_time_interval(last_line_item)
            )
        )


def _check_record_offset(old_line_item, new_line_item):
    old_line_item_id = _get_line_item_id(old_line_item)
    new_line_item_id = _get_line_item_id(new_line_item)
    old_line_item_time_interval = _get_line_item_time_interval(old_line_item)
    new_line_item_time_interval = _get_line_item_time_interval(new_line_item)

    if (old_line_item_id != new_line_item_id) or (old_line_item_time_interval != new_line_item_time_interval):
        _logger.error(
            'Record mismatch: {},{} != {},{}'.format(
                old_line_item_id,
                old_line_item_time_interval,
                new_line_item_id,
                new_line_item_time_interval
            )
        )


def _check_report_schema_change(headers, line_item):
    '''Compare a line_item doc with a set of headers.'''
    line_item.pop(LINE_ITEM_OFFSET_KEY)
    line_item_headers = []
    for k, v in line_item.items():
        for subk, subv in v.items():
            line_item_headers.append('/'.join([k, subk]))

    headers.sort()
    line_item_headers.sort()
    return headers == line_item_headers


def _check_s3_object_exists(s3_bucket, s3_key):
    '''Check if an object exists in S3'''
    resp = s3_client.list_objects(
        Bucket=s3_bucket,
        Prefix=s3_key
    )

    exists = False
    if 'Contents'in resp:
        for k in resp.get('Contents'):
            if k.get('Key') == s3_key:
                exists = True
                break

    return exists


def _convert_empty_value_to_none(item):
    '''Turn empty strings into None, etc.'''

    # DynamoDB can't have empty strings but csv.DictReader earlier in system
    # uses '' for empty fields.
    for key, value in item.items():
        if value == '':
            item[key] = None

    return item


def _create_line_item_message(headers, line_item, record_offset=None):
    '''Return a formatted line item message.'''
    split_line_item = line_item.split(',')
    item_dict = dict(zip(headers, split_line_item))
    if record_offset is not None:
        item_dict[LINE_ITEM_OFFSET_KEY] = record_offset
    sanitized_item_dict = _convert_empty_value_to_none(item_dict)

    final_dict = _format_line_item_dict(sanitized_item_dict)

    return final_dict


def _get_billing_report_start_datetime(headers, line_item):
    '''Get the start time from a line.'''
    line_item_dict = _create_line_item_message(headers, line_item)

    return _get_line_item_billing_period_start_datetime(line_item_dict)


def _get_last_record_offset(s3_bucket):
    # get item from last run if it exists
    last_line_item = _get_last_adm_line_item(s3_bucket)
    _logger.info('last_line_item: {}'.format(json.dumps(last_line_item)))

    if len(last_line_item) > 0:
        record_offset = _get_line_item_offset(last_line_item)
    else:
        record_offset = None

    return record_offset


def _get_line_item_billing_period_start_datetime(line_item):
    '''Get the '''
    time_string = line_item.get('bill').get('BillingPeriodStartDate')
    return iso8601.parse_date(time_string)


def _get_line_item_offset(line_item):
    '''Get the offset key of a line item'''
    return line_item.get(LINE_ITEM_OFFSET_KEY)


def _get_line_item_id(line_item):
    '''Get the '''
    return line_item.get('identity').get('LineItemId')


def _get_line_item_time_interval(line_item):
    '''Get the time interval of the line item'''
    return line_item.get('identity').get('TimeInterval')


def _get_last_adm_line_item(s3_bucket):
    '''Get the last ADM item from the last run.'''
    key = LAST_ADM_ITEM

    if _check_s3_object_exists(s3_bucket, key):
        line_item = json.loads(_get_s3_object_body(s3_bucket, key))
    else:
        line_item = {}

    return line_item


def _delete_s3_object(s3_bucket, s3_key):
    '''Get object body from S3.'''
    resp = s3_client.delete_object(
        Bucket=s3_bucket,
        Key=s3_key
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


def _format_line_item_dict(line_item_dict):
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
    event.get('Records')[0][X_RECORD_OFFSET] = record_offset
    resp = lambda_client.invoke(
        FunctionName=arn,
        Payload=json.dumps(event),
        InvocationType='Event'
    )
    # pop non-serializible value
    resp.pop('Payload')
    return resp


def _publish_sns_message(topic_arn, line_item):
    '''Publish message to SNS'''
    resp = sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(line_item)
    )

    return resp


def _put_last_adm_item(s3_bucket, line_item):
    '''Get the last ADM item from the last run.'''
    s3_key = LAST_ADM_ITEM
    resp = _put_s3_object(s3_bucket, s3_key, line_item)

    return resp


def _put_s3_object(s3_bucket, s3_key, line_item):
    '''Write item to S3'''
    resp = s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=json.dumps(line_item)
    )

    return resp


def handler(event, context):
    _logger.info('S3 event received: {}'.format(json.dumps(event)))
    s3_bucket = event.get('Records')[0].get('s3').get('bucket').get('name')
    s3_key = event.get('Records')[0].get('s3').get('object').get('key')
    record_offset = event.get('Records')[0].get(X_RECORD_OFFSET)

    s3_object_body = _get_s3_object_body(s3_bucket, s3_key)
    s3_body_file = io.StringIO(s3_object_body)

    # FIXME: This block has caused us to need to allocate more memory. We
    # should get more efficient with this.
    total_line_items = s3_body_file.readlines()
    # Get header so we can format messagesd
    len_total_line_items = len(total_line_items) - 1    # Account for header
    _logger.info('Total items: {}'.format(len_total_line_items))

    # Get header so we can format messages.
    record_headers = total_line_items[0].strip().split(',')


    # Check if a last run state file exists.
    if record_offset is None:
        last_line_item = _get_last_adm_line_item(s3_bucket)
        _logger.debug('last_line_item: {}'.format(json.dumps(last_line_item)))


    # Check for a billing report schema change. This causes some
    # identity.LineItemIds to change.
    if len(last_line_item) > 0:
        # Need to use a copy of the record header list or building items later fails.
        if not _check_report_schema_change(record_headers[:], last_line_item):
            raise BillingReportSchemaChange('Schema mismatch')


    # check if we're in a new month
    if record_offset is None and len(last_line_item) > 0:
        billing_report_start = _get_billing_report_start_datetime(record_headers, total_line_items[1].strip())
        last_line_item_start = _get_line_item_billing_period_start_datetime(last_line_item)

        # NOTE: We don't care about GT or LT. As a result, the state file will
        # always reflect the last run.  That means processing a previous
        # month, eg. manual invocation, will cause the current month's data to
        # be reprocessed.
        if last_line_item_start.month != billing_report_start.month:
            record_offset = 1

    # NOTE: I'm unsure if line item order in the report is persistent or if
    # values might change.
    if record_offset is None and len(last_line_item) > 0:
        # get item from last run if it exists
        last_line_item_offset = _get_line_item_offset(last_line_item)
        # FIXME: testing the data here to see if old data remains
        # consistent across reports.
        assumed_last_line_item = _create_line_item_message(record_headers, total_line_items[last_line_item_offset])
        _check_record_offset(last_line_item, assumed_last_line_item)

        if last_line_item_offset == len(total_line_items) - 1:
            _logger.info('Appears to be same report as last time.')
            record_offset = -1
        # There's more records since last known spot.
        elif last_line_item_offset < len(total_line_items):
            next_line_item = _create_line_item_message(record_headers, total_line_items[last_line_item_offset + 1])
            _check_next_record_date(last_line_item, next_line_item)
            record_offset = last_line_item_offset + 1
        # We shouldn't hit this unless we've processed reports out of order.
        # Just start from beginning if out of order.
        else:
            _logger.info(
                'Out of order report. Got offset {} but report has {} items. Processing anyways...'.format(
                        last_line_item_offset,
                        len(total_line_items) - 1   # Account for header record.
                )
            )
            record_offset = 1

    # There's last run record so we start from beginning.
    if record_offset is None:
        record_offset = 1

    # Here we check essentially if we're not processing the same report over.
    if record_offset > 0:
        line_items = total_line_items[record_offset:]
    else:
        line_items = []

    # NOTE: We might decide to batch send multiple records at a time.  It's
    # Worth a look after we have decent metrics to understand tradeoffs.
    sns_resp = []
    last_line_item_msg = None
    for line_item in line_items:
        _logger.info('Publishing line_item: {}'.format(record_offset))
        _logger.debug('line_item: {}'.format(line_item))

        stripped_line_item = line_item.strip()
        line_item_msg = _create_line_item_message(record_headers, stripped_line_item, record_offset)
        _logger.debug('message: {}'.format(json.dumps(line_item_msg)))

        resp = _publish_sns_message(AWS_SNS_TOPIC, line_item_msg)
        _logger.debug(
            'Publish response for line_item {}: {}'.format(
                record_offset, json.dumps(resp)
            )
        )

        sns_resp.append(resp)
        record_offset += 1
        # Keep track of this so when we break we know
        last_line_item_msg = line_item_msg

        if context.get_remaining_time_in_millis() <= 2000:
            break

    # Mark where we are.  Even if processing is not done, we at least have
    # this much known processed in case we
    if last_line_item_msg is not None:
        s3_last_item_resp = _put_last_adm_item(s3_bucket, last_line_item_msg)
    else:
        s3_last_item_resp = None

    # We're done.  Remove file.
    # FIXME: Need a better way to check for processing same report than using
    # -1 as the offset.
    if record_offset > 0 and record_offset < len_total_line_items:
        _logger.info('Invoking additional execution at record offset: {}'.format(record_offset))
        lambda_resp = _process_additional_items(context.invoked_function_arn, event, record_offset)
        _logger.info('Invoked additional Lambda response: {}'.format(json.dumps(lambda_resp)))

        s3_delete_resp = None
    else:
        s3_delete_resp = _delete_s3_object(s3_bucket, s3_key)
        lambda_resp = None
        _logger.info('No additional records to process')


    resp = {
        'sns': sns_resp,
        'lambda': lambda_resp,
        's3': {
            'report_delete': s3_delete_resp,
            'last_item_put': s3_last_item_resp
        },
    }

    _logger.info('AWS responses: {}'.format(json.dumps(resp)))
    return resp

