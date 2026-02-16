import logging
import sqlite3
import typing
from datetime import datetime as dt

from flexpart_ifs_preprocessor import CONFIG
from flexpart_ifs_preprocessor.domain.data_model import IFSForecast

logger = logging.getLogger(__name__)


class DB:
    conn: sqlite3.Connection

    def __init__(self) -> None:
        try:
            """Establish a database connection."""
            self.db_path = CONFIG.main.db_path
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.debug("Connected to database.")
            self._initialize_db()
        except sqlite3.Error as e:
            logger.exception(f"An error occurred: {e}")
            raise
        else:
            logger.debug("Database setup complete.")

    def _initialize_db(self) -> None:
        """Create tables if they do not exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS uploaded (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            forecast_ref_time TEXT NOT NULL,
            step INTEGER NOT NULL,
            key TEXT NOT NULL,
            processed BOOLEAN NOT NULL,
            UNIQUE(forecast_ref_time, step, key)
        )
        """
        try:
            with self.conn:
                self.conn.execute(create_table_query)
                logger.debug("Table uploaded is ready.")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while initializing the database: {e}")
            raise

    def insert_item(self, item: IFSForecast) -> None:
        """Insert a single item into the 'uploaded' table and update its row_id."""
        try:
            with self.conn:
                # Insert the item and get the newly inserted row_id
                result = self.conn.execute(
                    """
                    INSERT INTO uploaded (forecast_ref_time, step, key, processed)
                    VALUES (?, ?, ?, ?)
                    RETURNING row_id
                    """,
                    (
                        item.forecast_ref_time,
                        item.step,
                        item.key,
                        item.processed,
                    ),
                )

                # Fetch the row_id from the result and update the item's row_id
                item.row_id = result.fetchone()[0]

            logger.debug("Data inserted successfully")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while inserting data: {e}")
            raise

    def get_processable_steps(
        self, forecast_ref_time: dt
    ) -> list[list[dict[str, typing.Any]]]:
        """
        Query the database for unprocessed steps that can be processed, ensuring
        that at least two step=0 items exist and that each step has its previous
        step present before proceeding.

        Args:
            forecast_ref_time (datetime): The forecast reference time to query for.

        Returns:
            list[list[dict]]: A list of lists containing IFSForecast objects for
            each step, including two step-0 items and the previous step if step != 0.
        """
        try:
            # Ensure database connection is managed properly with context
            with self.conn:
                # Query for step-0 items (up to 2)
                step_zero_items = self._fetch_step_zero_items(forecast_ref_time)

                # Return if fewer than two step-0 items exist
                if len(step_zero_items) < 2:
                    logger.info(
                        f"Currently only {len(step_zero_items)} step=0 files are available. "
                        "Waiting for these before processing."
                    )
                    return []

                # Fetch the current and previous steps in a single query
                rows = self._fetch_current_and_previous_steps(forecast_ref_time)

                if not rows:
                    logger.info(
                        f"No timesteps available for forecast_ref_time={forecast_ref_time}."
                    )
                    return []

                logger.info(f"Query returned {len(rows)} pending timestep(s).")

                # Prepare and return the combined list of processable steps
                combined_steps = self._prepare_processable_steps(step_zero_items, rows)

                return combined_steps

        except sqlite3.Error as e:
            logger.exception(
                f"An error occurred while querying the database for forecast_ref_time={forecast_ref_time}: {e}"
            )
            raise

    def _fetch_step_zero_items(self, forecast_ref_time: dt) -> list:
        """
        Fetch step-0 items from the database.

        Args:
            forecast_ref_time (datetime): The forecast reference time to query for.

        Returns:
            list: A list of step-0 rows.
        """
        step_zero_query = (
            "SELECT row_id, forecast_ref_time, step, key, processed "
            "FROM uploaded "
            "WHERE forecast_ref_time = ? AND step = 0 "
            "LIMIT 2"
        )

        cursor = self.conn.execute(step_zero_query, (forecast_ref_time,))
        return cursor.fetchall()

    def _fetch_current_and_previous_steps(self, forecast_ref_time: dt) -> list:
        """
        Fetch current steps and their previous steps from the database.

        Args:
            forecast_ref_time (datetime): The forecast reference time to query for.

        Returns:
            list: A list of rows containing both the current step and its previous step.
        """
        tstart = CONFIG.main.time_settings.tstart
        tincr = CONFIG.main.time_settings.tincr

        step_query = """
        SELECT
            cur.row_id AS cur_row_id,
            cur.forecast_ref_time AS cur_forecast_ref_time,
            cur.step AS cur_step,
            cur.key AS cur_key,
            cur.processed AS cur_processed,
            prev.row_id AS prev_row_id,
            prev.forecast_ref_time AS prev_forecast_ref_time,
            prev.step AS prev_step,
            prev.key AS prev_key,
            prev.processed AS prev_processed
        FROM
            uploaded cur
        LEFT JOIN
            uploaded prev ON cur.forecast_ref_time = prev.forecast_ref_time
                        AND prev.step = cur.step - ?
                        -- Skip constants file (key ends in '11')
                        -- to avoid duplicate cur.step
                        -- as constants file also has prev.step = 0
                        AND (prev_step != 0 OR SUBSTR(prev.key, -2, 2) != '11')

        WHERE
            cur.forecast_ref_time = ? AND
            cur.processed = FALSE AND
            cur.step != 0 AND
            (cur.step - ?) % ? = 0 AND
            prev.step is not NULL
        ORDER BY
            cur.step;
        """

        cursor = self.conn.execute(
            step_query, (tincr, forecast_ref_time, tstart, tincr)
        )

        return cursor.fetchall()

    def _prepare_processable_steps(self, step_zero_items: list, rows: list) -> list:
        """
        Prepare the processable steps by combining step-0 items with the previous
        and current steps from the query results.

        Args:
            step_zero_items (list): The step-0 items to include in the processable steps.
            rows (list): The rows containing the current and previous steps.

        Returns:
            list: A combined list of processable steps.
        """
        combined_steps = []

        # Create step-0 IFSForecast objects
        step_zero_forecasts = [
            IFSForecast(
                row_id=zero_row["row_id"],
                forecast_ref_time=dt.strptime(
                    zero_row["forecast_ref_time"], "%Y-%m-%d %H:%M:%S"
                ),
                step=int(zero_row["step"]),
                key=zero_row["key"],
                processed=zero_row["processed"],
            ).to_dict()
            for zero_row in step_zero_items
        ]

        for row in rows:
            current_step = row[2]  # cur_step
            prev_step = current_step - CONFIG.main.time_settings.tincr

            # Start with step-0 items
            processable_list = step_zero_forecasts.copy()

            # If previous step is not step-0, add it
            if prev_step != 0:
                processable_list.append(
                    IFSForecast(
                        row_id=row["prev_row_id"],
                        forecast_ref_time=dt.strptime(
                            row["prev_forecast_ref_time"], "%Y-%m-%d %H:%M:%S"
                        ),
                        step=int(row["prev_step"]),
                        key=row["prev_key"],
                        processed=row["prev_processed"],
                    ).to_dict()
                )

            # Add the current step to the processable list
            processable_list.append(
                IFSForecast(
                    row_id=row["cur_row_id"],
                    forecast_ref_time=dt.strptime(
                        row["cur_forecast_ref_time"], "%Y-%m-%d %H:%M:%S"
                    ),
                    step=int(row["cur_step"]),
                    key=row["cur_key"],
                    processed=row["cur_processed"],
                ).to_dict()
            )

            combined_steps.append(processable_list)

        return combined_steps

    def update_item_as_processed(self, row_id: int) -> None:
        """Update the 'processed' field of a specific item to True."""
        try:
            with self.conn:
                result = self.conn.execute(
                    """
                    UPDATE uploaded
                    SET processed = 1
                    WHERE row_id = ?
                    """,
                    (row_id,),
                )
                if result.rowcount > 0:
                    logger.info("Item marked as processed.")
                else:
                    logger.warning("No item found to update.")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while updating the item: {e}")
            raise
