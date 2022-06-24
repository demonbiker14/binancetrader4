import asyncio
import logging
from functools import wraps

from binance import AsyncClient, BinanceSocketManager
from binance.client import BaseClient
from binance.streams import ReconnectingWebsocket

from config import API_KEY, TIMEOUT, API_SECRET, RETRY_COUNT
from trader import Trader


#  CORRECTING AIOHTTP ISSUE  #

def fix():
    from asyncio.proactor_events import _ProactorBasePipeTransport

    BaseClient.REQUEST_TIMEOUT = TIMEOUT
    ReconnectingWebsocket.MAX_RECONNECTS = RETRY_COUNT

    def silence_event_loop_closed(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except RuntimeError as e:
                if str(e) != 'Event loop is closed':
                    raise e

        return wrapper

    _ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)


fix()
##

logger = logging.getLogger('.')

if __name__ == '__main__':

    async def main():

        client = await AsyncClient.create(api_key=API_KEY, api_secret=API_SECRET)
        bsm = BinanceSocketManager(client=client, user_timeout=TIMEOUT)
        trader = Trader(
            client=client,
            bsm=bsm
        )
        a = await trader.process()
        print(a)
        await asyncio.sleep(5)


    try:
        asyncio.run(main())
    except Exception as exc:
        logger.exception(exc)
        raise exc
