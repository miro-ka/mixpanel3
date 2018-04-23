import sys
import logging
import configargparse
from mixpanel3.events import Events
import datetime

"""
Program splits large Events export (many days) to small daily batches.
"""

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def scoop(args):
    date_from_string = args.from_date
    date_to_string = args.to_date
    date_from = datetime.datetime.strptime(date_from_string, '%Y-%m-%d').date()
    date_to = datetime.datetime.strptime(date_to_string, '%Y-%m-%d').date()

    events_to_export = []
    if args.events:
        events_to_export = [args.events]

    api = Events(api_secret=args.api_secret)

    scoop_date = date_from
    while scoop_date <= date_to:

        logger.info("Fetching data for period: " + str(scoop_date))

        api.export(from_date=str(scoop_date),
                   to_date=str(scoop_date),
                   events_to_export=events_to_export,
                   output_dir=args.out_dir)

        scoop_date += datetime.timedelta(days=1)


def fetch_events(args):
    api = Events(api_secret=args.api_secret)
    events_to_export = []
    if args.events:
        events_to_export = [args.events]
    api.export(from_date=args.from_date,
               to_date=args.to_date,
               events_to_export=events_to_export,
               output_dir=args.out_dir)


if __name__ == '__main__':
    arg_parser = configargparse.get_argument_parser()
    input_args = arg_parser.parse_known_args()[0]
    scoop(input_args)

    logger.info('Application finished')
