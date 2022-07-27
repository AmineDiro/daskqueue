import os
import logging

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)

# Create handlers
c_handler = logging.StreamHandler()
# c_handler.setLevel(LOGLEVEL)
c_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
c_format = logging.Formatter("%(asctime)s,%(msecs)d %(levelname)s: %(message)s")
c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)
