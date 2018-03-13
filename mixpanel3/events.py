
import os
import sys
import urllib.request
from http.client import IncompleteRead
import time
import pandas as pd
import json
import logging
import configargparse
import cchardet
import base64
import requests


class Events(object):
    arg_parser = configargparse.get_argument_parser()
    arg_parser.add("--api_secret", help="Mixpanel API secret", required=True)
    arg_parser.add("--from_date", help="Export starting date (for ex. 2018-01-01)", required=True)
    arg_parser.add("--to_date", help="Export ending date (for ex. 2018-01-01)", required=True)
    arg_parser.add("--events", help="Events to be exported (comma separated)")
    arg_parser.add("--out_dir", help="Output directory", required=True)

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
        # self.from_date =
        self.logger = logging.getLogger(__name__)

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
                    if line_log_interval % line_log_interval == 0:
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
            # properties = event['properties']
            # properties.append(event['properties'])
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
            file_path = output_dir + file_name
            self.logger.info('..file converted: ' + file_name)
            df.to_csv(file_path)

        end_time = time.time()
        total_sim_time = int(end_time - start_time)
        self.logger.info('Conversion done in: ' + str(total_sim_time) + ' sec.')
