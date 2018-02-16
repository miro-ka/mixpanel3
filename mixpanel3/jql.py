import sys
import json
import logging
import http.client
import pandas as pd
import configargparse
import time
import base64


class JQL(object):
    arg_parser = configargparse.get_argument_parser()
    arg_parser.add("--api_secret", help="Mixpanel API secret", required=True)
    arg_parser.add("--from_date", help="Export starting date (for ex. 2018-01-01)", required=False)
    arg_parser.add("--to_date", help="Export ending date (for ex. 2018-02-01)", required=False)
    request_url = 'https://mixpanel.com/api/2.0/jql/'

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    def __init__(self, api_secret):
        self.api_secret = api_secret
        # self.from_date =
        self.logger = logging.getLogger(__name__)

    def run(self, jql_payload):
        """
        Functions sends request and returns result parsed as Pandas DataFrame
        Input: JQL query data in raw/text format URL encoded
        Returns pandas dataframe
        """
        start_time = int(time.time())
        conn = http.client.HTTPSConnection("mixpanel.com")
        b64val = base64.b64encode(self.api_secret.encode())

        headers = {
            'authorization': "Basic " + b64val.decode("utf-8")
        }

        conn.request("POST", "/api/2.0/jql/", jql_payload, headers)

        self.logger.info("Sending request..")
        res = conn.getresponse()
        self.logger.info("Request done. Result: " + str(res.code))

        if res.code != 200:
            self.logger.error("JQL run() Received not 200 result!")
            raise ValueError("JQL run() Received not 200 result!")

        self.logger.info("Parsing response to json..")
        data = res.read()
        json_res = json.loads(data)
        # json_res = res.json()
        self.logger.info("Loading json to pandas..")
        df = pd.DataFrame(json_res['results'])
        end_time = int(time.time())

        self.logger.info("Fetching and parsing done in (sec.): " + str(end_time-start_time))
        return df

