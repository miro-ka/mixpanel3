import logging
import sqlite3
import pandas as pd


class DB:
    logger = logging.getLogger(__name__)
    db_file = "db/mixpanel3_exports.sqlite"

    def __init__(self):
        self.logger.info("Starting db client")
        self.conn = sqlite3.connect(self.db_file)
        self.logger.info("Starting db client - done")

    def append(self, date_from, date_to, event_name, file_size):
        """
        :return: Logs all events including file_size
        """
        df = pd.DataFrame({'date_from': pd.Timestamp(date_from),
                           'date_to': pd.Timestamp(date_to),
                           'event': event_name,
                           'file_size': file_size},
                          index=[0])
        try:
            df.to_sql("exports", self.conn, if_exists="append", index=False)
        except pd.io.sql.DatabaseError as e:
            self.logger.warning("DatabaseError: Table currencies most probably does not exist", e)
            return None
