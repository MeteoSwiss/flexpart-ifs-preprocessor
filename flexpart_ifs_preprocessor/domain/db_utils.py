import logging
import os
from datetime import datetime, UTC

import boto3

from flexpart_ifs_preprocessor import CONFIG
from flexpart_ifs_preprocessor.domain.data_model import InputDataAggregatorEvent, IFSForecastFile

logger = logging.getLogger(__name__)

db_client = boto3.resource('dynamodb')

def write_product_index(input_data_aggregator_event: InputDataAggregatorEvent) -> None:

    # Adding created_at parameter for tracking files longevity
    creation_timestamp = int(datetime.now(UTC).timestamp())

    message = {
        'ReferenceTimePartitionKey': int(input_data_aggregator_event.forecast_ref_time.timestamp()),  # Partition Key
        'ObjectKey': input_data_aggregator_event.object_key,  # DynamoDB Primary Sort Key
        'ReferenceTime': str(input_data_aggregator_event.forecast_ref_time),
        'LeadTime': input_data_aggregator_event.step,
        'FileName': input_data_aggregator_event.filename,
        'CreatedAt': creation_timestamp,
        'Status': 'PENDING',
    }

    dynamodb_table = db_client.Table(os.environ['DYNAMODB_TABLE'])
    dynamodb_table.put_item(Item=message)


def get_steps_to_process(forecast_ref_time: datetime) -> tuple[list[tuple[IFSForecastFile, IFSForecastFile]], list[IFSForecastFile]]:

    dynamodb_table = db_client.Table(os.environ['DYNAMODB_TABLE'])
    all_response = dynamodb_table.query(
        KeyConditionExpression='ReferenceTimePartitionKey = :ref_time',
        ExpressionAttributeValues={':ref_time': int(forecast_ref_time.timestamp())}
    )
    all_items = all_response.get('Items', [])
    items_by_lead_time = {item['LeadTime']: item for item in all_items}  # fast lookup
    pending_items = [item for item in all_items if item['Status'] == 'PENDING']
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
        prev_lead_time = lead_time - CONFIG.main.time_settings.tincr

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
        forecast_ref_time=datetime.fromtimestamp(item['ReferenceTimePartitionKey'], tz=UTC),
        step=item['LeadTime'],
        object_key=item['ObjectKey'],
        filename=item['FileName'],
        processed=item['Status'] == 'PROCESSED'
    )

def update_product_index_processed(object_key: str, reference_time: datetime) -> None:
    processed_timestamp = int(datetime.now(UTC).timestamp())

    dynamodb_table = db_client.Table(os.environ['DYNAMODB_TABLE'])
    dynamodb_table.update_item(
        Key={
            'ReferenceTimePartitionKey': int(reference_time.timestamp()),
            'ObjectKey': object_key,
        },
        UpdateExpression='SET ProcessedAt = :processed_at, #s = :status',
        ExpressionAttributeNames={
            '#s': 'Status',  # 'Status' is a reserved word in DynamoDB
        },
        ExpressionAttributeValues={
            ':processed_at': processed_timestamp,
            ':status': 'PROCESSED',
        }
    )
