"""Pre-Process IFS HRES data as input to Flexpart."""

import json
import logging
import base64
import re
from typing import Any

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.kafka import ConsumerRecords

from flexpart_ifs_preprocessor.domain.data_model import  InputDataAggregatorEvent, Stream, Feed, IFSForecastFile
from flexpart_ifs_preprocessor.domain.db_utils import write_product_index, get_steps_to_process, update_product_index_processed
from flexpart_ifs_preprocessor.domain.processing import run_preprocessing

logger = logging.getLogger(__name__)


def lambda_handler(event: ConsumerRecords, _: LambdaContext) -> None:
    file_events = _parse_event_records(event)

    for file_event in file_events:
        logger.info('file_event.object_key: %s', file_event.object_key)
        logger.info('file_event.filename: %s', file_event.filename)
        logger.info('file_event.forecast_ref_time: %s', file_event.forecast_ref_time)
        logger.info('file_event.step: %s', file_event.step)

        write_product_index(file_event)

        processable_steps, step_zero_files = get_steps_to_process(file_event.forecast_ref_time)
        for file, prev_file in processable_steps:
            run_preprocessing(file, prev_file, step_zero_files)

            update_product_index_processed(file.object_key, file.forecast_ref_time)


def _kafka_event_to_input_data_aggregator_event(kafka_event: dict[str, Any]) -> InputDataAggregatorEvent:
    data = json.loads(base64.b64decode(kafka_event['value']))
    logger.debug('Event value: %s', data)

    return InputDataAggregatorEvent(data)


def _parse_event_records(event: ConsumerRecords) -> list[IFSForecastFile]:
    files = []

    for _, kafka_events in event['records'].items():
        for kafka_event in kafka_events:
            event_value = _kafka_event_to_input_data_aggregator_event(kafka_event)
            if event_value.stream in {Stream.S4Y, Stream.S5Y, Stream.S6Y} and event_value.feed in {Feed.F1, Feed.F2}:
            # Only process data coming from S4Y, S5Y or S6Y streams and feeds F1 and F2 as these are the only ones needed for Flexpart
                files.append(
                    IFSForecastFile(event_value.object_key, event_value.filename, event_value.feed)
                    )

    return files
