# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import logging


def log_info(msg):
    logging.info(msg)


def log_warning(msg):
    logging.warning(msg)

def log_error(msg):
    logging.error(msg)


class CustomLogger:
    def __init__(self, app, run_id):
        self.app = app
        self.run_id = run_id

    def log_info(self, msg):
        log_info(f'{self.app} | {self.run_id}: log with {msg}')

    def log_warning(self, msg):
        log_warning(f'{self.app} | {self.run_id}: warning with {msg}')

    def log_error(self, msg):
        log_error(f'{self.app} | {self.run_id}: error with {msg}')
