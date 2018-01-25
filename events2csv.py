import sys
import logging
import configargparse
from api.events import Events


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    arg_parser = configargparse.get_argument_parser()
    args = arg_parser.parse_known_args()[0]

    api = Events(api_secret=args.api_secret)
    api.export(from_date=args.from_date, to_date=args.to_date, events_to_export=[args.events])

    logger.info('Application finished')
