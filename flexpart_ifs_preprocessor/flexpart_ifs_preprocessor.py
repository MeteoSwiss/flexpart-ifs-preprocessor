"""Pre-Process IFS HRES data as input to Flexpart."""

import json
import logging
import base64
from typing import Any

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.kafka import ConsumerRecords

from flexpart_ifs_preprocessor.domain.data_model import  InputDataAggregatorEvent, Stream, Feed, IFSForecastFile
from flexpart_ifs_preprocessor.domain.db_utils import write_product_index, get_steps_to_process, update_product_index_processed
from flexpart_ifs_preprocessor.domain.processing import run_preprocessing

logger = logging.getLogger(__name__)


def lambda_handler(event: ConsumerRecords, _: LambdaContext) -> None:

    failed_items = []

    for _, kafka_records in event["records"].items():
        for kafka_record in kafka_records:
            try:

                event_value = _kafka_event_to_input_data_aggregator_event(kafka_record)
                if event_value.stream not in {Stream.S4Y, Stream.S5Y, Stream.S6Y} or event_value.feed not in {Feed.F1, Feed.F2}:
                    logger.info("Skipping record offset=%s, not a relevant stream/feed", kafka_record.get("offset"))
                    continue

                file_event = IFSForecastFile(event_value.object_key, event_value.filename, event_value.feed)

                logger.info('file_event.object_key: %s', file_event.object_key)
                logger.info('file_event.filename: %s', file_event.filename)
                logger.info('file_event.forecast_ref_time: %s', file_event.forecast_ref_time)
                logger.info('file_event.step: %s', file_event.step)

                write_product_index(file_event)

                # For F1 (GLOBAL) files, only 3-hourly preprocessing is needed, as FLEXPART
                # global runs use 3-hourly NWP data with precipitation averaged over 3 hours.
                #
                # For F2 (EUROPE) files, preprocessing is run twice:
                # - 1-hourly: for the FLEXPART-IFS-EUROPE domain (high-resolution regional runs)
                # - 3-hourly: for nesting the Europe domain into the global runs, which requires
                #   3-hourly NWP data with aggregated values (e.g. precipitation, fluxes) averaged
                #   over 3 hours.
                if file_event.domain == Feed.F1 and file_event.forecast_ref_time.hour in (0,12):
                    tincr_list = [3]
                elif file_event.domain == Feed.F2 and file_event.forecast_ref_time.hour in (0,12) and file_event.step % 3 == 0 :
                    tincr_list = [1,3]
                elif file_event.domain == Feed.F2:
                    tincr_list = [1]
                else:
                    logger.info("Skipping record offset=%s, not a relevant feed/forecast_datetime", kafka_record.get("offset"))
                    continue

                for tincr in tincr_list:

                    processable_steps, step_zero_files = get_steps_to_process(file_event.forecast_ref_time, file_event.domain, tincr)
                    for file, prev_file in processable_steps:
                        run_preprocessing(file, prev_file, step_zero_files, tincr)

                        update_product_index_processed(file.object_key, file.forecast_ref_time, tincr)

            except Exception as exc:
                logger.error(
                    "Failed to process record offset=%s topic=%s partition=%s: %s",
                    kafka_record.get("offset"),
                    kafka_record.get("topic"),
                    kafka_record.get("partition"),
                    exc,
                    exc_info=True,
                )
                failed_items.append({"itemIdentifier": kafka_record["eventID"]})

    return {"batchItemFailures": failed_items}

def _kafka_event_to_input_data_aggregator_event(kafka_event: dict[str, Any]) -> InputDataAggregatorEvent:
    data = json.loads(base64.b64decode(kafka_event['value']))
    logger.debug('Event value: %s', data)

    return InputDataAggregatorEvent(data)
