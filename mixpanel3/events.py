
import os
import sys
import time
import json
import logging
import cchardet
import base64
import requests
import hashlib
import sqlite3
from db.db import DB
import configargparse
import pandas as pd
import urllib.request
from http.client import IncompleteRead


class Events(object):
    arg_parser = configargparse.get_argument_parser()
    arg_parser.add("--api_secret", help="Mixpanel API secret", required=True)
    arg_parser.add("--from_date", help="Export starting date (for ex. 2018-01-01)", required=True)
    arg_parser.add("--to_date", help="Export ending date (for ex. 2018-01-01)", required=True)
    arg_parser.add("--events", help="Events to be exported (comma separated)")
    arg_parser.add("--out_dir", help="Output directory", required=True)
    arg_parser.add("--sqlite_logging", help="Log export progress to sqlite table (default False)", action='store_true',
                   default=False)
    arg_parser.add("--hash_distinct_id", help='Hash Distinct ID with sha256 + hash_backpack_string (default True)',
                   action='store_true', default=True)
    arg_parser.add("--hash_backpack_string", help="Extra hash string used for distinct id hashing")

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    to_date = ''
    from_date = ''
    max_retries = 10
    events_to_export = []
    api_version = '2.0'
    request_url = 'https://data.mixpanel.com/api'
    request_timeout = 300

    def __init__(self, api_secret):
        self.api_secret = api_secret
        self.logger = logging.getLogger(__name__)
        arg_parser = configargparse.get_argument_parser()
        args = arg_parser.parse_known_args()[0]
        self.hash_distinct_id = args.hash_distinct_id
        self.hash_backpack_string = args.hash_backpack_string
        self.sqlite_logging = args.sqlite_logging
        if self.sqlite_logging:
            self.db_client = DB()

    def export(self, from_date, to_date, events_to_export=[], output_dir='/out'):
        self.logger.info('Export started...')
        start_time = time.time()
        self.from_date = from_date
        self.to_date = to_date
        self.events_to_export = events_to_export

        # Build request json
        query_params = dict()
        query_params['event'] = events_to_export
        query_params['from_date'] = from_date
        query_params['to_date'] = to_date
        response = self.request(methods=['export'], params=query_params)
        self.events_to_csv(response, query_params, output_dir)

        end_time = time.time()
        total_sim_time = int(end_time - start_time)
        self.logger.info('Export done in: ' + str(total_sim_time) + ' sec.')

    def request(self, methods, params, http_method='GET', retries=0):
        """
        methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
                  will give us http://mixpanel.com/api/2.0/events/properties/values/
        params -  Extra parameters associated with method
        """
        start_time = time.time()
        self.logger.info('Sending data request for dates range: ' + params['from_date'] + ' - ' + params['to_date'])
        request_url = '/'.join([self.request_url, str(self.api_version)] + methods)
        if http_method == 'GET':
            data = None
            request_url = request_url + '/?' + self.unicode_urlencode(params)
        else:
            data = self.unicode_urlencode(params)

        b64val = base64.b64encode(self.api_secret.encode())

        headers = {
            'authorization': "Basic %s" % b64val.decode("utf-8")
        }

        res = requests.request("GET",
                               request_url,
                               data=data,
                               headers=headers,
                               stream=True)

        if res.status_code != 200:
            self.logger.error("JQL run() Received not 200 result!")
            raise ValueError("JQL run() Received not 200 result!")

        if retries <= self.max_retries:
            try:
                self.logger.info("Encoding response..")
                res_encoding_start = time.time()
                res.encoding = cchardet.detect(res.content)['encoding']
                self.logger.info("done in (sec): " + str(int(time.time() - res_encoding_start)))

                self.logger.info("Reading response..")
                res_read_time_start = time.time()
                str_response = res.text
                self.logger.info("done in (sec): " + str(int(time.time() - res_read_time_start)))

                self.logger.info("Splitting lines")
                lines = str_response.splitlines(True)
                self.logger.info("Splitting lines, done")

                end_time = time.time()
                total_sim_time = int(end_time - start_time)
                self.logger.info('Fetching done - total lines received: ' + str(len(lines)) +
                                 ' in: ' + str(total_sim_time) + ' sec.')
                records = []
                self.logger.info('Parsing strings to json...')
                line_counter = 0
                line_log_interval = 100000
                for line in lines:
                    if line_counter % line_log_interval == 0:
                        self.logger.info('Lines parsed to json: ' + str(line_counter) + ', out of: ' + str(len(lines)))
                    obj = json.loads(line)
                    records.append(obj)
                    line_counter += 1
                total_sim_time = int(end_time - start_time)
                self.logger.info('Parsing strings to json - done in: ' + str(total_sim_time) + ' sec.')
                return records

            except IncompleteRead as e:
                self.logger.warning('Got IncompleteRead exception!', e)
                self.request(methods=methods, params=params, http_method=http_method, retries=retries + 1)
                # str_response = e.partial.decode('utf8')
        else:
            self.logger.warning("Maximum retries reached. Request failed.")
            self.logger.warning("The server may be overloaded. Try again later.")
            return

    @staticmethod
    def unicode_urlencode(params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = list(params.items())
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params.remove(param)
                params.append((param[0], json.dumps(param[1]),))

        return urllib.parse.urlencode(
            [(k, v) for k, v in params]
        )

    def events_to_csv(self, data, params, output_dir):
        self.logger.info("Converting to csv (total of " + str(len(data)) + ' events)...')
        if len(data) == 0:
            self.logger.info("Nothing to do here..(got empty dataset)")
            return
        start_time = time.time()

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        event_sets = {}
        for event in data:
            event_name = event['event'].replace(" ", "_")
            if event_name in event_sets:
                event_sets[event_name].append(event['properties'])
            else:
                event_sets[event_name] = []
                event_sets[event_name].append(event['properties'])

        # Convert each event group to csv
        for key, value in event_sets.items():
            df = pd.DataFrame(value)
            dates_range = params['from_date'] + '_' + params['to_date']
            file_name = key + '_' + dates_range + '.csv'
            file_name = file_name.replace('-', '')
            file_path = output_dir + file_name
            self.logger.info('..file converted: ' + file_name)
            self.event_to_csv(df, file_path)

            # If sqlite logging is enabled log progress
            if self.sqlite_logging:
                self.db_client.append(params['from_date'],
                                      params['to_date'],
                                      file_name,
                                      df.values.nbytes/1048576)  # Convert to MB

        end_time = time.time()
        total_sim_time = int(end_time - start_time)
        self.logger.info('Conversion done in: ' + str(total_sim_time) + ' sec.')

    def event_to_csv(self, event_df, file_path):
        """
        Hashes distinct id (if set) and exports file to csv
        """
        if self.hash_distinct_id is True:
            event_df = self.hash_df(event_df, self.hash_backpack_string)
        event_df.to_csv(file_path, index=False)

    @staticmethod
    def hash_df(df, hash_backpack_string):
        """
        Converts distinct_id to hashed value
        """
        df['distinct_id'] = df['distinct_id'].apply(lambda x: hashlib.sha256((str(x) + hash_backpack_string).encode()).hexdigest())
        return df
