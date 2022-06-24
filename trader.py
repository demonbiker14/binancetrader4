import asyncio
import datetime
import logging
from decimal import Decimal
from typing import Dict

import aiohttp
import requests
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
from binance.streams import KeepAliveWebsocket

from config import RETRY_COUNT, BASE_ASSET, RETRY_TIMEOUT, NOTIFY_EVERY
from constants import Symbol, ISOLATED, is_stupid_error, run_binance_in_loop, check_ping
from settings_reader import Settings
from symbol_trader import SymbolTrader

logger = logging.getLogger('.')


class Trader:
    symbol_traders: Dict[Symbol, SymbolTrader]
    symbols: Dict[str, Symbol]
    client: AsyncClient
    bsm: BinanceSocketManager
    balance_lock: asyncio.Lock

    def __init__(self, client: AsyncClient, bsm: BinanceSocketManager):
        self.client = client
        self.bsm = bsm
        self.symbols = {}
        self.settings = Settings()
        self.balance_lock = asyncio.Lock()

    async def get_balance(self) -> Decimal:
        for i in range(RETRY_COUNT):
            try:
                data = await self.client.futures_account()
                return Decimal(next(filter(lambda x: x['asset'] == BASE_ASSET, data['assets']))['availableBalance'])
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                logger.exception(exc)
                if i == RETRY_COUNT - 1:
                    raise exc
            await asyncio.sleep(RETRY_TIMEOUT)

    async def prepare_symbols_list(self):
        resp = await self.client.futures_exchange_info()
        lst = resp['symbols']
        self.symbols = {
            x['symbol']: Symbol(base=x['baseAsset'], quote=x['quoteAsset'], symbol=x['symbol'])
            for x in lst if (
                    x['quoteAsset'] == 'USDT' and
                    x['status'] == 'TRADING' and
                    x['contractType'] == 'PERPETUAL'
            )}
        self.symbol_traders = {
            symbol: SymbolTrader(
                client=self.client, bsm=self.bsm, symbol=symbol, settings=self.settings,
                balance_lock=self.balance_lock
            ) for symbol_name, symbol in self.symbols.items()
        }

    async def prepare_position_mode(self):
        current_position_mode = await self.client.futures_get_position_mode()
        if not current_position_mode['dualSidePosition']:
            await self.client.futures_change_position_mode(dualSidePosition='true')

    async def prepare_margin_type(self):
        symbols_been = set()
        account_info = await self.client.futures_position_information()
        for position in account_info:
            if position['symbol'] not in self.symbols:
                continue
            symbol = self.symbols[position['symbol']]
            marginType = position['marginType']
            if marginType != 'isolated' and symbol not in symbols_been:
                await self.client.futures_change_margin_type(
                    symbol=symbol.symbol, marginType=ISOLATED

                )
                symbols_been.add(symbol)
                await asyncio.sleep(0.33)

    async def get_precisions(self) -> Dict[Symbol, int]:
        info = await self.client.futures_exchange_info()

        def get_tick_size(symbol_info: dict) -> Decimal:
            dat = list(filter(
                lambda x: x['filterType'] == 'PRICE_FILTER', symbol_info['filters']
            ))
            if dat:
                return Decimal(dat[0]['tickSize'])
            else:
                return Decimal('0.0')

        precisions = {
            self.symbols[symbol_info['symbol']]:
                {
                    'quantityPrecision': Decimal('10') ** -Decimal(symbol_info['quantityPrecision']),
                    'pricePrecision': Decimal('10') ** -Decimal(symbol_info['pricePrecision']),
                    'ticksize': get_tick_size(symbol_info),
                }
            for symbol_info in info['symbols'] if symbol_info['symbol'] in self.symbols
        }
        return precisions

    async def prepare(self):
        await self.prepare_symbols_list()
        await self.prepare_position_mode()
        await self.prepare_margin_type()
        precisions = await self.get_precisions()

        # GETTING MARK PRICES
        mark_prices_data = await self.client.futures_mark_price()
        mark_prices = {
            self.symbols[data['symbol']]: Decimal(data['markPrice']) for data in mark_prices_data
            if data['symbol'] in self.symbols
        }

        for symbol, symbol_trader in self.symbol_traders.items():
            symbol_trader.quantity_precision = precisions[symbol]['quantityPrecision']
            symbol_trader.price_precision = precisions[symbol]['pricePrecision']
            symbol_trader.tick_size = precisions[symbol]['ticksize']
            symbol_trader.last_mark_price = mark_prices[symbol]

        # PREPARE SYMBOL_TRADERS #
        tasks = asyncio.gather(*[
            symbol_trader.prepare() for symbol_name, symbol_trader in self.symbol_traders.items()
        ])
        try:
            await tasks
        except Exception as exc:
            logger.exception(exc)
            tasks.cancel()
            raise exc

    async def handle_bars(self):
        logger.debug('Started handling minute bars')
        tasks = asyncio.gather(*[
            run_binance_in_loop(symbol_trader.handle_bar, logger) for symbol_name, symbol_trader in self.symbol_traders.items()
        ])
        try:
            await tasks
        except Exception as exc:
            tasks.cancel()
            raise exc

    async def update_24bars(self):
        logger.debug('Started loading 24 bars')
        tasks = asyncio.gather(*[
            run_binance_in_loop(symbol_trader.update_24bars, logger) for symbol_name, symbol_trader in
            self.symbol_traders.items()
        ])
        try:
            await tasks
        except Exception as exc:
            tasks.cancel()
            raise exc

    async def handle_mark_prices(self):
        socket: KeepAliveWebsocket = self.bsm.all_mark_price_socket()
        async with socket:
            logger.debug('Started handling market prices...')
            while True:
                try:
                    msg = await socket.recv()
                    check_ping(msg, logger)
                    datas = (msg)['data']
                except asyncio.TimeoutError as exc:
                    logger.exception(exc)
                    await socket._run_reconnect()
                    continue
                except Exception as exc:
                    if is_stupid_error(exc):
                        await socket._run_reconnect()
                        continue
                    else:
                        raise exc

                tasks = []
                for data in datas:
                    if data['s'] in self.symbols:
                        symbol_trader = self.symbol_traders[self.symbols[data['s']]]
                        tasks.append(symbol_trader.handle_mark_price(
                            data=data
                        ))
                tasks = asyncio.gather(*tasks)
                try:
                    await tasks
                except Exception as exc:
                    tasks.cancel()
                    raise exc

    async def handle_order_updates(self):
        socket: KeepAliveWebsocket = self.bsm.futures_user_socket()
        async with socket:
            logger.debug('Started handling order updates...')
            while True:
                try:
                    msg = await socket.recv()
                    check_ping(msg, logger)
                except asyncio.TimeoutError:
                    await socket._run_reconnect()
                    continue
                except Exception as exc:
                    if is_stupid_error(exc):
                        await socket._run_reconnect()
                        continue
                    else:
                        raise exc
                if msg['e'] == 'ORDER_TRADE_UPDATE':
                    update = msg['o']
                    symbol_trader = self.symbol_traders[self.symbols[update['s']]]
                    await symbol_trader.handle_order_updates(update)

    async def check_for_deal(self):
        logger.debug('Started checking for deals')
        tasks = asyncio.gather(*[
            symbol_trader.check_for_deal() for symbol_name, symbol_trader in self.symbol_traders.items()
        ])
        try:
            await tasks
        except Exception as exc:
            tasks.cancel()
            raise exc

    async def notify_client(self):
        while True:
            for i in range(10):
                ip = None
                try:
                    async with aiohttp.ClientSession() as session:
                        ip = await (await session.get('https://api.ipify.org')).text()
                    break
                except Exception as exc:
                    continue
                await asyncio.sleep(RETRY_TIMEOUT)
            else:
                ip = None

            if not ip:
                ip = 'Could not reach'
            logger.info(
                f'\nI am really working at {datetime.datetime.now()}\n'
                f'IP address: {ip}\n'
            )
            await asyncio.sleep(NOTIFY_EVERY)

    async def process(self):
        logger.debug('Preparing bot...')
        await self.prepare()
        logger.debug('Finished preparation.')

        balance = await self.get_balance()
        logger.debug(f'Available balance before the start: {str(balance)} {BASE_ASSET}')

        tasks = asyncio.gather(*[
            self.handle_bars(),
            run_binance_in_loop(self.handle_mark_prices, logger),
            run_binance_in_loop(self.handle_order_updates, logger),
            self.update_24bars(),
            self.check_for_deal(),
            self.notify_client()
        ])

        try:
            await tasks
        except Exception as exc:
            logger.exception(exc)
            tasks.cancel()
            raise exc
