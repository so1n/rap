import multiprocessing
import time

from .client import run_client
from .server import run_server

if __name__ == "__main__":
    import logging

    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%y-%m-%d %H:%M:%S", level=logging.INFO
    )
    p = multiprocessing.Process(target=run_server)
    p.start()
    time.sleep(1)
    run_client()
    time.sleep(1)
    p.terminate()
