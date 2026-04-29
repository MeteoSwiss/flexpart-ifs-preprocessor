import logging
from datetime import datetime, UTC

import boto3

from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile, Feed
from flexpart_ifs_preprocessor import CONFIG

logger = logging.getLogger(__name__)


def write_product_index(event: IFSForecastFile) -> None:
    """Write a new entry to the DynamoDB product index for the given file event."""

    # Adding created_at parameter for tracking files longevity
    creation_timestamp = int(datetime.now(UTC).timestamp())

    # TTL set to 2 days from now
    ttl_timestamp = creation_timestamp + (2 * 24 * 60 * 60)

    message = {
        'ReferenceTimePartitionKey': int(event.forecast_ref_time.timestamp()),  # Partition Key
        'ObjectKey': event.object_key,  # DynamoDB Primary Sort Key
        'ReferenceTime': str(event.forecast_ref_time),
        'LeadTime': event.step,
        'FileName': event.filename,
        'Domain': str(event.domain.value),
        'CreatedAt': creation_timestamp,
        'TTL': ttl_timestamp,
        f'Status_3h': 'PENDING',
        f'Status_1h': 'PENDING',
    }
    if event.domain == Feed.F1:
        # For F1 (GLOBAL) files, only 3-hourly preprocessing is needed, so we can set the 1-hourly status to 'N/A'.
        message['Status_1h'] = 'N/A'
    elif event.domain == Feed.F2 and event.step % 3 != 0:
        # For F2 (EUROPE) files that are not on 3-hourly steps, only 1-hourly preprocessing is needed, so we can set the 3-hourly status to 'N/A'.
        message['Status_3h'] = 'N/A'

    db_client = boto3.resource('dynamodb')
    dynamodb_table = db_client.Table(CONFIG.main.dynamodb_table_name)
    dynamodb_table.put_item(Item=message)


def get_steps_to_process(forecast_ref_time: datetime, domain: Feed, tincr: int = 1) -> tuple[list[tuple[IFSForecastFile, IFSForecastFile]], list[IFSForecastFile]]:
    """Query the DynamoDB product index for the given forecast reference time
    and return a list of (file, previous_file) tuples to process,
    along with a list of step=0 files."""

    db_client = boto3.resource('dynamodb')
    dynamodb_table = db_client.Table(CONFIG.main.dynamodb_table_name)
    all_response = dynamodb_table.query(
        KeyConditionExpression='ReferenceTimePartitionKey = :ref_time',
        FilterExpression='#domain = :domain',
        ExpressionAttributeNames={'#domain': 'Domain'},
        ExpressionAttributeValues={
            ':ref_time': int(forecast_ref_time.timestamp()),
            ':domain': str(domain.value),
        }
    )
    all_items = all_response.get('Items', [])
    items_by_lead_time = {item['LeadTime']: item for item in all_items}  # fast lookup
    pending_items = [item for item in all_items if item[f'Status_{tincr}h'] == 'PENDING']
    step_zero_items = [dynamodb_item_to_ifs_forecast_file(item) for item in all_items if item['LeadTime'] == 0]

    items_to_process = []

    logger.info(f"Queried DynamoDB for forecast_ref_time={forecast_ref_time}. Found {len(all_items)} item(s).")

    if len(step_zero_items) < 2:
        logger.info(
            f"Only {len(step_zero_items)} step=0 files are available for forecast_ref_time={forecast_ref_time}. "
            "Waiting for at least 2 step=0 files before processing."
        )
        return [], []

    for item in pending_items:
        lead_time = item['LeadTime']
        prev_lead_time = lead_time - tincr

        logger.debug(f"Unprocessed item: ObjectKey={item['ObjectKey']}, LeadTime={item['LeadTime']}, CreatedAt={item['CreatedAt']}")

        if prev_lead_time >= 0:
            prev_item = items_by_lead_time.get(prev_lead_time)
            if prev_item is None:
                logger.info(
                    f"Skipping item with ObjectKey={item['ObjectKey']} and LeadTime={item['LeadTime']} because "
                    f"previous lead time {prev_lead_time} is missing for forecast_ref_time={forecast_ref_time}."
                )
                continue
            logger.debug(f"Queueing item: ObjectKey={item['ObjectKey']}, LeadTime={lead_time}")
            items_to_process.append((dynamodb_item_to_ifs_forecast_file(item), dynamodb_item_to_ifs_forecast_file(prev_item)))

    return items_to_process, step_zero_items

def dynamodb_item_to_ifs_forecast_file(item: dict) -> IFSForecastFile:
    return IFSForecastFile(
        forecast_ref_time=datetime.fromtimestamp(int(item['ReferenceTimePartitionKey']), tz=UTC),
        step=item['LeadTime'],
        object_key=item['ObjectKey'],
        filename=item['FileName'],
        domain=Feed(item['Domain'])
    )

def update_product_index_processed(object_key: str, reference_time: datetime, tincr: int = 1) -> None:
    processed_timestamp = int(datetime.now(UTC).timestamp())

    db_client = boto3.resource('dynamodb')
    dynamodb_table = db_client.Table(CONFIG.main.dynamodb_table_name)
    dynamodb_table.update_item(
        Key={
            'ReferenceTimePartitionKey': int(reference_time.timestamp()),
            'ObjectKey': object_key,
        },
        UpdateExpression=f'SET ProcessedAt = :processed_at, Status_{tincr}h = :status',
        ExpressionAttributeValues={
            ':processed_at': processed_timestamp,
            ':status': 'PROCESSED',
        }
    )
