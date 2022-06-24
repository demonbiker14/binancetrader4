import logging
import os
import sys
from configparser import ConfigParser
from logging.handlers import RotatingFileHandler

from binance.enums import KLINE_INTERVAL_1MINUTE


CONFIG_FILE = 'config.ini'
config = ConfigParser()
config.read(CONFIG_FILE)
API_KEY = config.get('DEFAULT', 'API_KEY')
API_SECRET = config.get('DEFAULT', 'API_SECRET')

# API_KEY = os.getenv('API_KEY')
# API_SECRET = os.getenv('API_SECRET')

TIMEOUT = 25
TIMEFRAME = KLINE_INTERVAL_1MINUTE
BASE_ASSET = 'USDT'

SETTINGS_FILE = 'settings.ini'
SETTINGS_RELOAD = 60
CHECK_FOR_ENTERING_AVAILABILITY = 15
RETRY_TIMEOUT = 5
RETRY_COUNT = 5
KK_RESERVE = 15
TRADING = True
WAIT_TILL_TRADING = 20
NOTIFY_EVERY = 60
PING_TIMEOUT = 30
TRAIL_SPLIT = 0.05

# seconds
ORDER_RETRY = 0.5

# USDT
MINIMUM_ORDER = 5

LOGGER_FILE = 'log.log'
LOG_FOLDER = 'logs'
if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)
logger = logging.getLogger('.')
logger.setLevel(logging.DEBUG)
logger.propagate = True
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.addHandler(RotatingFileHandler(LOGGER_FILE, mode='w'))
