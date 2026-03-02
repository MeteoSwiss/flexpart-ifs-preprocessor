"""Pre-Process IFS HRES data as input to Flexpart."""

import json
import logging
import sys
import base64
import re
from datetime import datetime, timezone
from pathlib import Path

from flexpart_ifs_preprocessor.domain.data_model import IFSForecastFile, InputDataAggregatorEvent
from flexpart_ifs_preprocessor.domain.db_utils import write_product_index, get_steps_to_process, update_product_index_processed
from flexpart_ifs_preprocessor.domain.processing import run_preprocessing
from flexpart_ifs_preprocessor.domain.s3_utils import upload_to_s3

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    input_data_aggregator_events = _parse_event_records(event)

    for input_data_aggregator_event in input_data_aggregator_events:
        logger.info('input_data_aggregator_event.object_key: %s', input_data_aggregator_event.object_key)
        logger.info('input_data_aggregator_event.filename: %s', input_data_aggregator_event.filename)
        logger.info('input_data_aggregator_event.forecast_ref_time: %s', input_data_aggregator_event.forecast_ref_time)
        logger.info('input_data_aggregator_event.step: %s', input_data_aggregator_event.step)

        write_product_index(input_data_aggregator_event)

        processable_steps, step_zero_files = get_steps_to_process(input_data_aggregator_event.forecast_ref_time)
        for file, prev_file in processable_steps:
            run_preprocessing(file, prev_file, step_zero_files)

            update_product_index_processed(file.object_key, file.forecast_ref_time)


def _kafka_event_to_input_data_aggregator_event(kafka_event) -> InputDataAggregatorEvent | None:
    data = json.loads(base64.b64decode(kafka_event['value']))

    return InputDataAggregatorEvent(data)


def _parse_event_records(event) -> list[InputDataAggregatorEvent]:
    input_data_aggregator_events = []

    for _, kafka_events in event['records'].items():
        for kafka_event in kafka_events:
            input_data_aggregator_events.append(_kafka_event_to_input_data_aggregator_event(kafka_event))

    return input_data_aggregator_events
