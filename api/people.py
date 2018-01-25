import sys
import csv
import json
import time
import urllib
import logging
import urllib.request
import urllib.parse
import urllib.error
import urllib.request
import urllib.error
import urllib.parse
import configargparse
from base64 import b64encode


class People(object):

    arg_parser = configargparse.get_argument_parser()
    arg_parser.add("--api_secret", help="Mixpanel API secret", required=True)
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    def __init__(self, api_secret):
        self.api_secret = api_secret
        self.logger = logging.getLogger(__name__)

    def request(self, params):
        """
        Build mixpanel APi Request
        """
        data = None
        request_url = 'https://mixpanel.com/api/2.0/engage/?'
        request_url = request_url + self.unicode_urlencode(params)
        api_secret_byte = bytes(self.api_secret + ':', "utf-8")
        api_secret_ascii = b64encode(api_secret_byte).decode("ascii")
        headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=api_secret_ascii)}
        request = urllib.request.Request(request_url, data, headers)
        response = urllib.request.urlopen(request, timeout=120)
        return response.read()

    @staticmethod
    def unicode_urlencode(params):
        """
        Converts data to json format and correctly handle unicode url parameters
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.parse.urlencode([(k, isinstance(v, str) and v.encode('utf-8') or v) for k, v in params])
        return result

    def get_and_write_results(self, params):
        start_time = time.time()
        response = str(self.request(params), 'utf-8')
        params['session_id'] = json.loads(response)['session_id']
        params['page'] = 0
        global_total = json.loads(response)['total']

        self.logger.info("Session id is %s \n" % params['session_id'])
        self.logger.info("Here are the # of people %d" % global_total)

        paged = self.page_results(response, params, global_total)
        self.export_csv("people_export_" + str(int(time.time())) + ".csv", paged)
        end_time = time.time()
        total_sim_time = int(end_time - start_time)
        self.logger.info('Export done in total (sec): ' + str(total_sim_time))

    def page_results(self, response, parameters, global_total):
        """
        Builds/Writes results into txt files/pages
        """
        file_name = "people_export_" + str(int(time.time())) + ".txt"
        parameters['page'] = 0
        has_results = True
        total = 0
        while has_results:
            responser = json.loads(response)['results']
            total += len(responser)
            has_results = len(responser) == 1000
            self._write_results(responser, file_name)
            self.logger.info("%d / %d" % (total, global_total))
            parameters['page'] += 1
            if has_results:
                response = self.request(parameters)
        return file_name

    @staticmethod
    def _write_results(results, file_name):
        with open(file_name, 'a') as f:
            for data in results:
                f.write(json.dumps(data) + '\n')

    @staticmethod
    def export_csv(file_name_out, file_name_in):
        """
        takes a file name of a file of json objects and the desired name of the csv file
        that will be written
        """
        subkeys = set()
        with open(file_name_in, 'r') as r:
            with open(file_name_out, 'w') as w:
                # Get all properties (will use this to create the header)
                for line in r:
                    try:
                        subkeys.update(set(json.loads(line)['$properties'].keys()))
                    except:
                        pass

                # Create the header
                header = ['$distinct_id']
                for key in subkeys:
                    header.append(key)

                # Create the writer and write the header
                writer = csv.writer(w)
                writer.writerow(header)

                # Return to the top of the file, then write the events out, one per row
                r.seek(0, 0)
                for line in r:
                    entry = json.loads(line)
                    row = []
                    try:
                        row.append(entry['$distinct_id'])
                    except:
                        row.append('')

                    for sub_key in subkeys:
                        try:
                            row.append((entry['$properties'][sub_key]))
                        except KeyError:
                            row.append("")
                    writer.writerow(row)
