import argparse
import asyncio
import json
from typing import Dict, List, Tuple, Union

from rap.client import Client
from rap.client.processor import CryptoProcessor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--secret_key", default=None, help="conn server secret key")
    parser.add_argument("-m", "--mode", help="`d` display func list, `r` run func", choices=["d", "r"])
    parser.add_argument("-k", "--key", help="secret key")

    parser.add_argument("-n", "--name", help="func name")
    parser.add_argument("-a", "--arg", help="func param", default=tuple())
    parser.add_argument("-g", "--group", help="func group", default="default")
    args, unknown = parser.parse_known_args()
    secret_key: str = args.secret_key
    mode: str = args.mode
    func_name: str = args.name
    arg: Union[str, list] = args.arg
    group: str = args.group

    if isinstance(arg, str):
        arg_list: List[str] = arg.split(",")
    else:
        arg_list = arg

    loop = asyncio.get_event_loop()
    client = Client()
    if secret_key:
        crypto_key_id, crypto_key = secret_key.split(",")
        client.load_processor([CryptoProcessor(crypto_key_id, crypto_key)])
    loop.run_until_complete(client.connect())

    if mode == "d":
        result_tuple: Tuple[Dict[str, str]] = loop.run_until_complete(client.raw_call("list", group="registry"))
        print(json.dumps(result_tuple, indent=2))
        # column_list: List[str] = ["Name", "Group", "Type", "Path", "Module"]
        # display_table_list: List[List[str]] = [column_list]
        # for func_info in result_tuple:
        #     func_key, module_str, path_str = func_info
        #     func_group, func_type, func_name = func_key.split(":")
        #     display_table_list.append([func_name, func_group, func_type, path_str, module_str])
        # print_table(display_table_list)
    elif mode == "r" and func_name:
        print(loop.run_until_complete(client.raw_call(func_name, arg_param=arg_list, group=group)))

    loop.run_until_complete(client.await_close())
