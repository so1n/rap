import argparse
import asyncio
import sys
from rap.client import Client


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--secret_key", default=None, help="conn server secret key")
    parser.add_argument("-l", "--list", help="print func list")
    parser.add_argument("-r", "--run", help="run func")
    parser.add_argument("-a", "--arg", help="func param")
    args, unknown = parser.parse_known_args()
    secret_key: str = args.secret_key
    display_func_list = args.list
    run: str = args.run
    arg: str = args.arg

    loop = asyncio.get_event_loop()
    client = Client(secret_tuple=tuple(secret_key.split(",")))
    loop.run_until_complete(client.connect())
    if display_func_list:
        loop.run_until_complete(client.raw_call("_root_list"))
    elif run and arg:
        loop.run_until_complete(client.raw_call(run, arg))
    elif run:
        loop.run_until_complete(client.raw_call(run))
    else:
        loop.run_until_complete(client.wait_close())
        sys.exit("you should use -l or -r and -a")
    loop.run_until_complete(client.wait_close())
