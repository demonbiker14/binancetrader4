import asyncio
import enum
import logging
import sys
import typing
from asyncio import CancelledError
from datetime import datetime, timedelta
from decimal import Decimal
from typing import NamedTuple

import aiohttp.client_exceptions
from binance.enums import FUTURE_ORDER_TYPE_LIMIT, FUTURE_ORDER_TYPE_MARKET, \
    FUTURE_ORDER_TYPE_STOP, FUTURE_ORDER_TYPE_TAKE_PROFIT, FUTURE_ORDER_TYPE_STOP_MARKET
from binance.exceptions import BinanceAPIException

from config import RETRY_TIMEOUT, LOGGER_FILE, PING_TIMEOUT

ISOLATED = 'ISOLATED'
CROSS = 'CROSS'


def get_seconds_to_next_hour():
    now = datetime.now()
    next_hour = now.replace(second=0, microsecond=0, minute=0) + timedelta(hours=1)
    return (next_hour - now).total_seconds()


class Symbol(NamedTuple):
    base: str
    quote: str
    symbol: str


class Bar(NamedTuple):
    open: Decimal
    close: Decimal
    high: Decimal
    low: Decimal
    volume: Decimal
    timestamp: datetime


class OrderType(enum.Enum):
    MARKET = FUTURE_ORDER_TYPE_MARKET
    LIMIT = FUTURE_ORDER_TYPE_LIMIT
    STOP_LOSS = FUTURE_ORDER_TYPE_STOP
    STOP_LOSS_MARKET = FUTURE_ORDER_TYPE_STOP_MARKET
    TAKE_PROFIT = FUTURE_ORDER_TYPE_TAKE_PROFIT
    TRAILING_STOP = 'TRAILING_STOP_MARKET'

    def __str__(self):
        return self.value


class PositionSide(enum.Enum):
    LONG = 'LONG'
    SHORT = 'SHORT'

    def __str__(self):
        return self.value


class Side(enum.Enum):
    BUY = 'BUY'
    SELL = 'SELL'

    def __str__(self):
        return self.value


def is_stupid_error(exc: Exception) -> bool:
    if isinstance(exc, OSError):
        if exc.errno in {
            10038,  # operation on non-socket on Windows, likely because fd == -1
            121,  # the semaphore timeout period has expired on Windows
            1232,  # Сетевая папка недоступна.
        }:
            return True
    if isinstance(exc, aiohttp.client_exceptions.ClientConnectorError):
        return True
    return False


class StopTryingSignal(Exception):
    pass


class OrderWouldImmediatelyTriggerError(Exception):
    pass


async def run_binance_in_loop(callable: typing.Callable, logger: logging.Logger, args: typing.Optional[list] = None,
                              kwargs: typing.Optional[dict] = None):
    if not args:
        args = []
    if not kwargs:
        kwargs = {}
    while True:
        try:
            await callable(*args, **kwargs)
        except CancelledError as err:
            raise err
        except (BinanceAPIException, asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError) as exc:
            logger.exception(exc)
        except Exception as exc:
            raise exc
        await asyncio.sleep(RETRY_TIMEOUT)


def check_ping(msg: dict, logger: logging.Logger):
    E = msg.get('E')
    if E:
        timestamp = datetime.fromtimestamp(int(E) // 1000)
        now = datetime.now()
        delta_seconds = abs((now - timestamp).total_seconds())
        if PING_TIMEOUT != -1 and delta_seconds >= PING_TIMEOUT:
            logger.error(f'PING {str(delta_seconds)}s more than {str(PING_TIMEOUT)} seconds')
