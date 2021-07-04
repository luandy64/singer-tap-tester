# Set up a root module logger
import logging
import sys
LOGGER = logging.getLogger(__name__)
# Disconnect our logger from the root logger
LOGGER.parent = None
formatter = logging.Formatter(fmt='%(name)s %(levelname)s %(message)s', datefmt='')
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(formatter)
LOGGER.addHandler(handler)

# Hoist Important Classes and functions for convenience
from . import user
from .base import BaseTapTest, StandardTests
