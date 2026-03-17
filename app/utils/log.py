from __future__ import annotations

import errno
import logging
import logging.handlers
import sys
import time
from multiprocessing import Process
from multiprocessing.queues import Queue as MPQueue
from typing import Optional

from app.utils.singleton import Singleton


class Log(metaclass=Singleton):
    listener: Optional[Process] = None

    @classmethod
    def logging_process(cls, q: MPQueue[logging.LogRecord]) -> None:
        cls.configure_queue_logger()
        while True:
            try:
                while not q.empty():
                    record = q.get()
                    if record is None:
                        return
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
            except IOError as e:
                if e.errno == errno.EPIPE:
                    print("EPIPE")
            time.sleep(0.1)

    @classmethod
    def configure_queue_logger(cls) -> None:
        root = logging.getLogger()
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    @classmethod
    def configure_main_logger(
        cls, q: MPQueue[logging.LogRecord]
    ) -> logging.Logger:
        logger = logging.getLogger("main")
        logger.addHandler(logging.handlers.QueueHandler(q))
        logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def configure_process_logger(
        cls,
        q: MPQueue[logging.LogRecord],
        variable: str,
        member: int,
    ) -> logging.Logger:
        logger = logging.getLogger(f"worker-{variable}-{member}")
        logger.addHandler(logging.handlers.QueueHandler(q))
        logger.setLevel(logging.INFO)
        return logger

    @classmethod
    def start_logging_process(cls, q: MPQueue[logging.LogRecord]) -> None:
        cls.listener = Process(target=cls.logging_process, args=(q,))
        cls.listener.start()

    @classmethod
    def terminate_logging_process(cls) -> None:
        if cls.listener is not None:
            cls.listener.terminate()
