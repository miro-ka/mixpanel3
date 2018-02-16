import time
import configargparse
from mixpanel3.jql import JQL

if __name__ == '__main__':
    arg_parser = configargparse.get_argument_parser()
    args = arg_parser.parse_known_args()[0]

    start_time = time.time()

    jql = JQL(args.api_secret)
    res = jql.run(jql_payload=args.jql_payload)

    end_time = time.time()
    duration = int(end_time-start_time)
    print('Program done in (sec):', duration)
