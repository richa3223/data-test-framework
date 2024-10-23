import logging
import os
import sys
import time

from typing import Callable, Tuple


class TerminalColours:
    def supports_colour():
        """
        Returns True if the running system's terminal supports color, and False
        otherwise.
        """
        plat = sys.platform
        supported_platform = plat != "Pocket PC" and (
            plat != "win32" or "ANSICON" in os.environ
        )
        is_a_tty = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
        return supported_platform and is_a_tty

    PURPLE = "\033[95m" if supports_colour() else ""
    BLUE = "\033[94m" if supports_colour() else ""
    CYAN = "\033[96m" if supports_colour() else ""
    GRAY = "\33[90m" if supports_colour() else ""
    GREEN = "\033[92m" if supports_colour() else ""
    YELLOW = "\033[93m" if supports_colour() else ""
    RED = "\033[91m" if supports_colour() else ""
    END = "\33[0m" if supports_colour() else ""


class RetryBackoffIterator:


    def __init__(
        self,
        func: Callable,
        args: Tuple,
        exception_type: Exception,
        start_time_s: int,
        n_tries: int,
        max_time: int = 30,
    ):
        self._func = func
        self._args = args
        self._exception_type = exception_type
        self._start_time_s = start_time_s
        self._n_tries = n_tries
        self._max_time = max_time

    def __iter__(self):
        self._done = False
        self._current_try = 1
        self._next_wait = self._start_time_s
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        try:
            self._done = True
            return self._func(*self._args)
        except self._exception_type as e:
            if self._current_try > self._n_tries:
                logging.exception("Caught Exception during function, no more retries")
                raise e
            self._done = False
            logging.exception("Caught Exception during function")
            logging.warning(
                f"Retrying again in {self._next_wait} seconds. Try {self._current_try}/{self._n_tries}"
            )
            time.sleep(self._next_wait)
            self._next_wait = min(
                (
                    self._next_wait + 1
                    if self._next_wait <= 1
                    else self._next_wait * self._next_wait
                ),
                self._max_time,
            )
            self._current_try += 1


def parse_str_table(table_with_headers):
    list_table_rows = table_with_headers.split("\n")
    return [
        tuple(map(str.strip, str(row).strip().strip("|").split("|")))
        for row in list_table_rows[1:]
    ]


def prefix_string(string: str, prefix: str) -> str:
    if not prefix:
        return string
    return f"{prefix}{string}"


def prefix_path(path: str, prefix: str) -> str:
    if not prefix:
        return path
    if path.startswith("/"):
        return f"/{prefix}{path}"
    return f"{prefix}/{path}"
