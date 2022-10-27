import argparse
import asyncio
import json
from typing import Dict, Tuple

from rap.client import Client
from rap.client.endpoint import LocalEndpointProvider
from rap.client.processor import CryptoProcessor
from rap.common.utils import constant

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-h", "--server_host", help="server host", default="localhost")
    parser.add_argument("-p", "--server_port", help="server port", default="9000")
    parser.add_argument("-s", "--secret_key", default=None, help="transport server secret key")
    parser.add_argument("-m", "--mode", help="`d` display func list, `r` run func", choices=["d", "r"])
    parser.add_argument("-k", "--key", help="secret key")

    parser.add_argument("-n", "--name", help="func name")
    parser.add_argument("-a", "--arg", help="func param", default=None)
    parser.add_argument("-g", "--group", help="func group", default=constant.DEFAULT_GROUP)
    args, unknown = parser.parse_known_args()
    server_host: str = args.server_host
    server_port: str = args.server_port
    secret_key: str = args.secret_key
    mode: str = args.mode
    func_name: str = args.name
    arg: str = args.arg
    group: str = args.group

    if isinstance(arg, str):
        arg_dict: dict = json.loads(arg)
    else:
        arg_dict = {}

    loop = asyncio.get_event_loop()

    client: Client = Client(
        endpoint_provider=LocalEndpointProvider.build({"ip": server_host, "port": int(server_port)})
    )
    if secret_key:
        crypto_key_id, crypto_key = secret_key.split(",")
        client.load_processor(CryptoProcessor(crypto_key_id, crypto_key))
    loop.run_until_complete(client.start())

    if mode == "d":
        result_tuple: Tuple[Dict[str, str]] = loop.run_until_complete(client.invoke_by_name("list", group="registry"))
        print(json.dumps(result_tuple, indent=2))
        # column_list: List[str] = ["Name", "Group", "Type", "Path", "Module"]
        # display_table_list: List[List[str]] = [column_list]
        # for func_info in result_tuple:
        #     func_key, module_str, path_str = func_info
        #     func_group, func_type, target = func_key.split(":")
        #     display_table_list.append([target, func_group, func_type, path_str, module_str])
        # print_table(display_table_list)
    elif mode == "r" and func_name:
        print(loop.run_until_complete(client.invoke_by_name(func_name, param=arg_dict, group=group)))

    loop.run_until_complete(client.stop())
