"""
Script for exporting MixPanel users data to csv

Credits: wgins (https://gist.github.com/wgins/b21f4f0c2e160f7f95af)
"""

import sys
import time
import logging
import configargparse
from api.people import People

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    arg_parser = configargparse.get_argument_parser()
    args = arg_parser.parse_known_args()[0]

    api = People(api_secret=args.api_secret)

    # TODO: All parameters needs to be moved to mixpanel-api
    """
    Here is the place to define your selector to target only the users that you're after
    selector = '(datetime(1458587013 - 86400) > properties["Created"] and behaviors["behavior_79"] > 0'
    behaviors = '[{"window": "90d", "name": "behavior_79", "event_selectors": [{"event": "Edit Colors"}]}]'
    
    # Leave 'r' before the behaviors and selector strings so that they are interpreted as a string
    # literals to handle escaped quotes
    """
    selector = r''
    behaviors = r''

    """
    Optionally export just specific properties. Exports will always include $distinct_id and $last_seen
    output_properties = ['property1', 'property2', 'property3']
    """
    output_properties = []

    if not behaviors:
        params = {'selector': selector}
    else:
        time_offset = int(input("Project time offset from GMT (ex. PST = -8): "))
        params = {'selector': selector,
                  'behaviors': behaviors,
                  'as_of_timestamp': int(time.time()) + (time_offset * 3600)}

    if output_properties:
        params['output_properties'] = output_properties

    logger.info("Starting with export..")
    api.get_and_write_results(params)
