import sys
import logging
import configargparse
from mixpanel3.events import Events


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


events = []  # Add names of events here which you want to export


def fetch_events(args):
    api = Events(api_secret=args.api_secret)

    for event in events:
        logger.info("Fetching event:" + event)
        event_to_fetch = [event]
        api.export(from_date=args.from_date,
                   to_date=args.to_date,
                   events_to_export=event_to_fetch,
                   output_dir=args.out_dir)


if __name__ == '__main__':
    arg_parser = configargparse.get_argument_parser()
    input_args = arg_parser.parse_known_args()[0]
    fetch_events(input_args)

    logger.info('Application finished')
