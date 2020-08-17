import argparse
import asyncio
import sys
from rap.client import Client


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--secret_key", default=None,
        help="conn server secret key"
    )
    parser.add_argument(
        "-l", "--list",
        help="print func list"
    )
    parser.add_argument(
        "-r", "--run",
        help="run func"
    )
    parser.add_argument(
        "-a", "--arg",
        help="func param"
    )
    args, unknown = parser.parse_known_args()
    secret_key = args.secret_key
    display_func_list = args.list
    run = args.run
    arg = args.arg

    loop = asyncio.get_event_loop()
    client = Client(secret=secret_key)
    loop.run_until_complete(client.connect())
    print(loop.run_until_complete(client.call_by_text('_root_list')))
    if display_func_list:
        loop.run_until_complete(client.call_by_text('_root_list'))
    elif run and arg:
        loop.run_until_complete(client.call_by_text(run, arg))
    elif run:
        loop.run_until_complete(client.call_by_text(run))
    else:
        client.close()
        sys.exit('you should use -l or -r and -a')
    client.close()
