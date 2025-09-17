"""Pre-Process IFS HRES data as input to Flexpart."""

import argparse
import logging
import sys
from datetime import datetime as dt
from pathlib import Path

from flexprep.domain.data_model import IFSForecast
from flexprep.domain.db_utils import DB
from flexprep.domain.processing import Processing

logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Parse metadata of new file received")
    parser.add_argument("--step", type=str, required=True, help="Step argument")
    parser.add_argument(
        "--date", type=str, required=True, help="Date argument (yyyymmdd)"
    )
    parser.add_argument("--time", type=str, required=True, help="Time argument (HH)")
    parser.add_argument("--location", type=str, required=True, help="Location argument")

    return parser.parse_args()


def create_forecast_object_from_args(args):
    """Create an IFSForecast object based on the parsed arguments."""
    try:
        forecast_ref_time_str = f"{args.date}{int(args.time):02d}00"
        forecast_ref_time = dt.strptime(forecast_ref_time_str, "%Y%m%d%H%M")
        return IFSForecast(
            row_id=None,
            forecast_ref_time=forecast_ref_time,
            step=int(args.step),
            key=Path(args.location).name,
            processed=False,
        )
    except ValueError as ve:
        logger.error(f"Invalid date or time format: {ve}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error while creating forecast object: {e}")
        sys.exit(1)


def insert_forecast_in_db(ifs_forecast_obj, db):
    """Insert an IFSForecast object into the database."""
    try:
        db.insert_item(ifs_forecast_obj)
        logger.info(
            f"Successfully inserted item ({ifs_forecast_obj.forecast_ref_time}, "
            f"Step: {ifs_forecast_obj.step}, Key: {ifs_forecast_obj.key})"
        )
    except Exception as e:
        logger.error(f"Failed to insert item into the database: {e}")
        sys.exit(1)


def get_processable_forecasts(args, db):
    """Insert forecast in DB and find processable steps."""
    # Create the forecast object and insert it into the DB
    ifs_forecast_obj = create_forecast_object_from_args(args)
    insert_forecast_in_db(ifs_forecast_obj, db)

    # Get the processable steps
    processable_steps = db.get_processable_steps(ifs_forecast_obj.forecast_ref_time)
    return processable_steps


if __name__ == "__main__":
    args = parse_arguments()
    logger.info(
        f"Notification received for file - Step: {args.step}, Date: {args.date}, "
        f"Time: {args.time}, Location: {args.location}"
    )

    db = DB()
    processable_steps = get_processable_forecasts(args, db)
    for to_process in processable_steps:
        Processing().process(to_process)
