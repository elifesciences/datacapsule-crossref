import logging

import pytest


@pytest.fixture(autouse=True, scope='session')
def configure_logging():
    logging.root.handlers = []
    logging.basicConfig(level='DEBUG')
