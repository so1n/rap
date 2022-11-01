import multiprocessing
import time

from .api_client import run_api_client
from .api_server import run_api_server
from .server import run_server

if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )
    p1 = multiprocessing.Process(target=run_server)
    p2 = multiprocessing.Process(target=run_api_server)
    p1.start()
    p2.start()
    time.sleep(1)
    run_api_client()
    time.sleep(1)
    p1.terminate()
    p2.terminate()
