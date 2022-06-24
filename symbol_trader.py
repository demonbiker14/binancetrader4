import asyncio
import datetime
import logging
import typing
from asyncio import Lock
from decimal import Decimal
from typing import List, Optional, Dict, Any

from binance import AsyncClient, BinanceSocketManager
from binance.enums import KLINE_INTERVAL_1HOUR, ORDER_RESP_TYPE_RESULT
from binance.exceptions import BinanceAPIException

from calculations import Calculator
from config import TIMEFRAME, CHECK_FOR_ENTERING_AVAILABILITY, RETRY_TIMEOUT, BASE_ASSET, KK_RESERVE, MINIMUM_ORDER, \
    RETRY_COUNT, TRADING, WAIT_TILL_TRADING, ORDER_RETRY
from constants import Symbol, Bar, OrderType, PositionSide, get_seconds_to_next_hour, Side, is_stupid_error, \
    StopTryingSignal, OrderWouldImmediatelyTriggerError, check_ping
from deal_info import DealInfo
from settings_reader import Settings


class SymbolTrader:
    client: AsyncClient
    bsm: BinanceSocketManager
    long_calculator: Calculator
    short_calculator: Calculator

    settings_dump: dict
    symbols: Symbol

    bars: List[Bar]
    history: List[Bar]
    recent_bars24: List[Bar]

    pause_till: Optional[datetime.datetime]
    last_mark_price: Decimal
    current_bar: Optional[Bar]
    deal_info: Optional[DealInfo]

    quantity_precision: Decimal
    price_precision: Decimal
    tick_size: Decimal

    opened: bool  # any opened position
    leverage_set: bool = False
    stop_trying: bool

    balance_lock: asyncio.Lock
    order_lock: Lock

    last_realized_profit: Decimal

    hanging_price_events: Dict[
        typing.Tuple[Decimal, PositionSide], asyncio.Event]  # {(mark price, side): asyncio.Event}
    hanging_order_events: Dict[int, asyncio.Event]  # {order id: asyncio.Event}
    orders_data: Dict[int, dict]  # { order id: order data }
    order_ids: Dict[str, int]  # { order name: order id }

    def __init__(
            self,
            client: AsyncClient,
            bsm: BinanceSocketManager,
            symbol: Symbol,
            settings: Settings,
            balance_lock: asyncio.Lock,
    ):
        self.client = client
        self.bsm = bsm

        self.symbol = symbol

        self.settings = settings
        self.settings_dump = self.settings.get_dump()

        self.pause_till = None
        self.stop_trying = False

        self.quantity_precision = Decimal('1.0')
        self.price_precision = Decimal('1.0')

        self.long_calculator = Calculator(position_side=PositionSide.LONG)
        self.short_calculator = Calculator(position_side=PositionSide.SHORT)
        self.current_bar = None
        self.deal_info = None

        self.hanging_order_events = {}
        self.hanging_price_events = {}
        self.orders_data = {}
        self.order_ids = {}
        self.last_realized_profit = Decimal(0)

        self.balance_lock = balance_lock
        self.order_lock = asyncio.Lock()

        self.logger = logging.getLogger(f'.')

        self.depth = self.get_needed_depth()
        self.opened = False

    def get_needed_depth(self):
        return max(
            self.settings_dump['doji'] + 10,
            self.settings_dump['minute_K_long'] + KK_RESERVE,
            self.settings_dump['minute_K_short'] + KK_RESERVE,
            self.settings_dump['minute_W_long'] + KK_RESERVE,
            self.settings_dump['minute_W_short'] + KK_RESERVE,
        )

    def add_order_event(self, order_id: int):
        event = asyncio.Event()
        self.hanging_order_events[order_id] = event
        return event

    def add_price_event(self, mark_price: Decimal, side: PositionSide):
        event = asyncio.Event()
        self.hanging_price_events[(mark_price, side)] = event
        return event

    def add_order_info(self, order_id, data):
        self.orders_data[order_id] = data

    def deal_log(self, msg):
        new_msg = f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n' \
                  f'{msg}'

    def clear_deal_data(self):
        self.long_calculator.clear()
        self.short_calculator.clear()

        self.leverage_set = False
        self.stop_trying = False
        self.opened = False

        self.last_realized_profit = Decimal('0.0')

        self.hanging_price_events.clear()
        self.hanging_order_events.clear()
        self.orders_data.clear()
        self.order_ids.clear()

    def set_pause(self):
        delay = self.settings_dump['pause_N_minute']
        self.pause_till = datetime.datetime.now() + datetime.timedelta(minutes=delay)

    async def get_balance(self) -> Decimal:
        for i in range(RETRY_COUNT):
            try:
                data = await self.client.futures_account()
                return Decimal(next(filter(lambda x: x['asset'] == BASE_ASSET, data['assets']))['availableBalance'])
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                self.logger.exception(exc)
                if i == RETRY_COUNT - 1:
                    raise StopTryingSignal()
            except Exception as exc:
                if is_stupid_error(exc):
                    continue
                else:
                    raise exc
            await asyncio.sleep(RETRY_TIMEOUT)

    def need_to_hold(self) -> bool:
        # VOL24, DOJI, BLACKLIST, PAUSE
        vol24 = Calculator.volume24(self.recent_bars24)
        doji = Calculator.get_doji(self.history, self.settings_dump['doji'])
        is_in_blacklist = self.symbol.symbol in self.settings_dump['blacklist']
        if self.pause_till:
            pause_minute = datetime.datetime.now() < self.pause_till
        else:
            pause_minute = False
        if (vol24 < self.settings_dump['min_vol_usdt']) or doji:
            self.logger.debug('\n========')
            if doji:
                self.logger.debug(
                    f'{str(datetime.datetime.now())}\n'
                    f'{self.symbol.symbol} not entered because of hold-list DOJI\n'
                )
            if vol24 < self.settings_dump['min_vol_usdt']:
                min_vol_usdt = self.settings_dump['min_vol_usdt']
                self.logger.debug(
                    f'{str(datetime.datetime.now())}\n'
                    f'{self.symbol.symbol} not entered because of hold-list VOL24\n'
                    f'Volume 24h {str(vol24)}{BASE_ASSET} < {str(min_vol_usdt)}'
                )
            # if pause_minute:
            #     self.logger.debug(f'{self.symbol.symbol} not entered because of hold-list PAUSE_MINUTE')

            self.logger.debug('========\n')
        return (vol24 < self.settings_dump['min_vol_usdt']) or doji or is_in_blacklist or pause_minute

    async def change_leverage(self, leverage):
        for i in range(RETRY_COUNT):
            try:
                await self.client.futures_change_leverage(
                    symbol=self.symbol.symbol,
                    leverage=leverage
                )
                return
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                self.logger.exception(exc)
                if i == RETRY_COUNT - 1:
                    raise exc
            except Exception as exc:
                if is_stupid_error(exc):
                    continue
                else:
                    raise exc
            await asyncio.sleep(RETRY_TIMEOUT)

    async def change_leverage_if_not_deal(self, leverage: int):
        if self.leverage_set:
            return None
        return await self.change_leverage(leverage)

    async def post_order(
            self,
            order_type: OrderType,
            side: Side,
            position_side: PositionSide,
            quantity: Optional[Decimal] = None,
            price: Optional[Decimal] = None,
            stop_price: Optional[Decimal] = None,
            activation_price: Optional[Decimal] = None,
            callback_rate: Optional[Decimal] = None,
            new_order_resp_type: Optional[str] = None
    ) -> dict:
        i = 0
        while i <= RETRY_COUNT - 1:
            try:
                order = await self.client.futures_create_order(
                    symbol=self.symbol.symbol,
                    side=str(side),
                    positionSide=str(position_side),
                    type=str(order_type),
                    quantity=str(quantity) if quantity else None,
                    stopPrice=str(stop_price) if stop_price else None,
                    callbackRate=str(callback_rate) if callback_rate else None,
                    activationPrice=str(activation_price) if activation_price else None,
                    price=str(price) if price else None,
                    newOrderRespType=new_order_resp_type,
                )
                return order
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                if isinstance(exc, BinanceAPIException) and exc.code == -2021:
                    self.logger.error(
                        f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                        f'Error with {self.symbol.symbol} : order {str(order_type)} would immediately trigger')
                    if i == RETRY_COUNT - 1:
                        raise OrderWouldImmediatelyTriggerError()
                elif self.stop_trying:
                    raise StopTryingSignal()
                else:
                    self.logger.exception(exc)
                if i == RETRY_COUNT - 1:
                    raise exc
            except Exception as exc:
                if is_stupid_error(exc):
                    continue
                else:
                    raise exc
            i += 1
            await asyncio.sleep(ORDER_RETRY)

    async def cancel_order(self, order_id: int):
        for i in range(RETRY_COUNT):
            try:
                await self.client.futures_cancel_order(
                    symbol=self.symbol.symbol,
                    orderId=order_id,
                )
                return
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                if type(exc) == BinanceAPIException and exc.code == -2011:
                    return
                self.logger.exception(exc)

                if i == RETRY_COUNT - 1:
                    raise exc
            except Exception as exc:
                if is_stupid_error(exc):
                    continue
                else:
                    raise exc
            await asyncio.sleep(RETRY_TIMEOUT)

    async def preload_bars(self):
        for i in range(RETRY_COUNT):
            try:
                klines = await self.client.futures_klines(symbol=self.symbol.symbol, interval=TIMEFRAME,
                                                          limit=self.depth + 10)
                break
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                self.logger.exception(exc)
                if i == RETRY_COUNT - 1:
                    raise exc
            except Exception as exc:
                if is_stupid_error(exc):
                    continue
                else:
                    raise exc
            await asyncio.sleep(RETRY_TIMEOUT)

        history = []
        for kline in klines[-self.depth - 2:-2]:
            timestamp, open_p, high_p, low_p, close_p, volume = map(Decimal, kline[:6])
            bar = Bar(
                open=open_p,
                close=close_p,
                low=low_p,
                high=high_p,
                volume=volume,
                timestamp=datetime.datetime.fromtimestamp(int(timestamp) // 1000),
            )
            history.append(bar)
        self.history = history.copy()

    async def preload_24(self):
        for i in range(RETRY_COUNT):
            try:
                klines24 = await self.client.futures_klines(symbol=self.symbol.symbol, interval=KLINE_INTERVAL_1HOUR,
                                                            limit=25)
                break
            except (BinanceAPIException, asyncio.TimeoutError) as exc:
                self.logger.exception(exc)
                if i == RETRY_COUNT - 1:
                    raise exc
            except Exception as exc:
                if is_stupid_error(exc):
                    continue
                else:
                    raise exc
            await asyncio.sleep(RETRY_TIMEOUT)
        recent_bars24 = []
        for kline in klines24[:-1]:
            timestamp, open_p, high_p, low_p, close_p, volume = map(Decimal, kline[0:6])
            bar = Bar(
                open=open_p,
                close=close_p,
                low=low_p,
                high=high_p,
                volume=volume,
                timestamp=datetime.datetime.fromtimestamp(int(timestamp) // 1000),
            )
            recent_bars24.append(bar)
        self.recent_bars24 = recent_bars24.copy()

    async def prepare(self):
        # PRELOAD TIMEFRAME BARS
        await self.preload_bars()

        # PRELOAD 24h BARS
        await self.preload_24()

    async def check_for_deal(self):
        await asyncio.sleep(WAIT_TILL_TRADING)
        while True:
            self.settings_dump = self.settings.get_dump()
            need_to_hold = self.need_to_hold()
            if not need_to_hold and not self.opened and self.current_bar:
                # GET "CHANGE" FIRST
                if self.depth < self.get_needed_depth():
                    self.depth = self.get_needed_depth()
                    await self.preload_bars()

                mark_price = self.last_mark_price
                long_change = self.long_calculator.get_change(
                    bars=self.history + [self.current_bar],
                    KK=self.settings_dump['minute_K_long'],
                    JJ=self.settings_dump['change_J_long'],
                    WW=self.settings_dump['minute_W_long'],
                    SPIKE=self.settings_dump['spike'],
                    current_price=mark_price,
                )
                short_change = self.short_calculator.get_change(
                    bars=self.history + [self.current_bar],
                    KK=self.settings_dump['minute_K_short'],
                    JJ=self.settings_dump['change_J_short'],
                    WW=self.settings_dump['minute_W_short'],
                    SPIKE=self.settings_dump['spike'],
                    current_price=mark_price,
                )

                if long_change or short_change:
                    await self.balance_lock.acquire()
                    try:
                        balance = await self.get_balance()
                    except StopTryingSignal:
                        await asyncio.sleep(CHECK_FOR_ENTERING_AVAILABILITY)
                        continue
                    except Exception as exc:
                        raise exc
                    if not balance:
                        continue
                    if balance >= self.settings_dump['min_balance']:
                        deal_task = None

                        if long_change and self.settings_dump['lot_D_buy']:
                            deal_task = self.enter_deal(
                                position_side=PositionSide.LONG
                            )
                        elif short_change and self.settings_dump['lot_D_sell']:
                            deal_task = self.enter_deal(
                                position_side=PositionSide.SHORT

                            )
                        self.balance_lock.release()
                        if TRADING and deal_task:
                            await deal_task
                    else:
                        # NOT ENOUGH BALANCE
                        self.balance_lock.release()
            await asyncio.sleep(CHECK_FOR_ENTERING_AVAILABILITY)

    async def enter_deal(self, position_side: PositionSide):
        self.opened = True
        async with self.balance_lock:
            self.deal_info = DealInfo(self.symbol, position_side)
            try:
                balance = await self.get_balance()
            except StopTryingSignal as exc:
                self.clear_deal_data()
                return None
            except Exception as exc:
                self.clear_deal_data()
                raise exc
            if balance <= self.settings_dump['min_balance']:
                # NOT ENOUGH BALANCE
                self.clear_deal_data()
                return None

            # SETTING LEVERAGE
            E_LEVERAGE = self.settings_dump['E_leverage']
            leverage = Calculator.get_leverage(
                E_leverage=E_LEVERAGE,
                balance=balance,
                min_leverage=self.settings_dump['min_leverage'],
                max_leverage=self.settings_dump['max_leverage'],
            )
            #

            # SET QUANTITY
            lot_D = self.settings_dump[
                'lot_D_buy' if position_side == PositionSide.LONG else 'lot_D_sell'
            ]
            quantity = Calculator.get_quantity(
                balance=balance,
                lot_D=lot_D,
                tick=MINIMUM_ORDER,
                leverage=leverage,
                quantity_precision=self.quantity_precision,
                price=self.last_mark_price
            )
            base_asset_quantity = quantity * self.last_mark_price

            if base_asset_quantity / leverage >= balance:
                # not enough balance
                self.logger.error(
                    f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                    f'Margin is insufficient for {quantity} {self.symbol.quote}\n')
                return None

            # ENTERING WITH MARKET
            await self.change_leverage_if_not_deal(leverage)
            self.leverage_set = True
            price = await self.set_market(position_side, quantity)

        sl0 = self.settings_dump['initial_SL0']

        split_slh = self.settings_dump['split_SLH_percent']
        sign = 1 if position_side == PositionSide.LONG else -1
        split_slh_price = Calculator.round_to_tick(price * (1 + sign * split_slh), self.tick_size)

        async with self.order_lock:
            trailing_order_event, tr_price = await self.set_trailing(position_side, price, quantity)
            trail_order_id = self.order_ids['TRAILING']
            trail_price_event = self.add_price_event(tr_price, position_side)
            split_trail_event = self.add_price_event(split_slh_price, position_side)

            if sl0:
                stop_order_event, stoploss_price = await self.set_stoploss(position_side, price, quantity)
                stoploss_order_id = self.order_ids['STOP_ORDER']
            else:
                stop_order_event, stoploss_price = None, None
                stoploss_order_id = -1
            moving_price_task = asyncio.create_task(
                self.move_stoploss(price, quantity, trail_price_event, position_side)
            )
            split_trail_task = asyncio.create_task(
                self.split_trail(price, quantity, split_trail_event, position_side)
            )

        calculator = self.long_calculator if position_side == PositionSide.LONG else self.short_calculator
        now = datetime.datetime.now()

        # ADDING DEAL INFO
        self.deal_info.open_deal(
            enter_price=price,
            trail_price=tr_price,
            stoploss_price=stoploss_price,
            quantity=quantity,
            base_asset_quantity=base_asset_quantity,
            change=calculator.percent_difference,
            change_price=calculator.CHECK_PRICE,
            settings_dump=self.settings_dump.copy(),
            leverage=leverage,
            trail_order_id=trail_order_id,
            stoploss_order_id=stoploss_order_id,
            enter_time=now,
        )

        # LOGGING
        KK = self.settings_dump['minute_K_long' if position_side == PositionSide.LONG else 'minute_K_short']
        JJ = self.settings_dump['change_J_long' if position_side == PositionSide.LONG else 'change_J_short']
        WW = self.settings_dump['minute_W_long' if position_side == PositionSide.LONG else 'minute_W_short']
        CHANGE = calculator.percent_difference.quantize(Decimal('0.01'))
        CALCULATION_CURRENT_PRICE = calculator.CHECK_PRICE.quantize(Decimal('0.01'))
        BASE_QUANTITY = base_asset_quantity.quantize(Decimal('0.01'))
        TR0 = self.settings_dump['trail_percent']
        TR_DISTANCE = self.settings_dump['trail_SL_distance']

        depth = len(self.history)
        history_datetimes = list(map(lambda bar: bar.timestamp.strftime('%H:%M'), self.history))
        history_txt = '\n'.join([f'{str(depth - i + 1)}: {history_datetimes[i]}' for i in range(depth)])

        self.logger.debug(
            f'========  {str(now)} =======\n'
            f'Entered {str(position_side)}: {self.symbol.symbol} (x{leverage}) {str(BASE_QUANTITY)} {BASE_ASSET}\n'
            f'Change {str(CHANGE)}% (J={str(JJ)}), K={str(KK)}, W={str(WW)}\n'
            f'Current price: {CALCULATION_CURRENT_PRICE}\n'
            f'AVG IN: {str(price)}\n'
            f'SL0: {str(stoploss_price)} (-{str(sl0)}%)\n'
            f'TR0: {str(tr_price)} (+{str(TR0)}%), distance {str(TR_DISTANCE)}%\n'
            f'History: \n{history_txt}\n'
            f'\n'
        )
        ###

        await self.close_deal(position_side, leverage, moving_price_task, trailing_order_event, stop_order_event)
        self.clear_deal_data()
        # pause invert not finished
        self.set_pause()
        self.order_lock.release()

    async def close_deal(
            self, position_side: PositionSide, leverage: int, moving_price_task: asyncio.Task,
            trailing_order_event: asyncio.Event, stop_order_event: Optional[asyncio.Event],

    ):
        stop_order_wait = asyncio.create_task(stop_order_event.wait()) if stop_order_event else None
        trailing_order_wait = asyncio.create_task(trailing_order_event.wait())

        wait = asyncio.wait([
                                trailing_order_wait,
                            ] + ([stop_order_wait] if stop_order_wait else []),
                            return_when=asyncio.FIRST_COMPLETED)

        done, pending = await wait

        await self.order_lock.acquire()
        closed_by = None
        if trailing_order_wait in pending:
            trailing_order_event.set()
            order_id = self.order_ids['TRAILING']
            await self.cancel_order(order_id)
            self.logger.info(
                f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                f'TRAIL order {order_id} for {self.symbol.symbol} cancelled\n'
            )
            closed_by = 'STOPLOSS'

        if stop_order_wait in pending:
            stop_order_event.set()
            order_id = self.order_ids['STOP_ORDER']
            await self.cancel_order(order_id)
            self.logger.info(
                f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                f'STOPLOSS order {str(order_id)} for {self.symbol.symbol} cancelled\n')
            closed_by = 'TRAILING'

        if not (moving_price_task.cancelled() or moving_price_task.done()):
            moving_price_task.cancel()

        PROFIT = self.last_realized_profit.quantize(Decimal('0.01'))
        self.logger.debug(
            f'========  {str(datetime.datetime.now())} =======\n'
            f'EXIT {str(position_side)}: {self.symbol.symbol} (x{str(leverage)})\n'
            f'order: {str(closed_by)} Profit: {PROFIT} {BASE_ASSET}\n'
        )

    async def set_market(self, position_side: PositionSide, quantity: Decimal) -> Decimal:
        """ returns AvgPrice """
        market_order_info = await self.post_order(
            order_type=OrderType.MARKET,
            quantity=quantity,
            side=Side.BUY if position_side == PositionSide.LONG else Side.SELL,
            position_side=position_side,
            new_order_resp_type=ORDER_RESP_TYPE_RESULT,
        )
        price = Decimal(market_order_info['avgPrice'])
        self.logger.debug(
            f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
            f'MARKET {str(position_side)} order for {self.symbol.symbol} filled\n'
            f'AvgPrice {str(price)}\n')
        return price

    async def set_stoploss(self, position_side: PositionSide, price: Decimal, quantity: Decimal
                           ) -> typing.Tuple[asyncio.Event, Decimal]:
        sl0 = self.settings_dump['initial_SL0']
        sign = 1 if position_side == PositionSide.LONG else -1
        sl_stop_price = Calculator.round_to_tick(
            price * (1 - sl0 / 100 * sign), self.tick_size
        )
        new_price = sl_stop_price - sign * self.tick_size
        try:
            stop_loss_order_info = await self.post_order(
                order_type=OrderType.STOP_LOSS,
                stop_price=sl_stop_price,
                price=new_price,
                position_side=position_side,
                side=Side.SELL if position_side == PositionSide.LONG else Side.BUY,
                quantity=quantity
            )
            stop_loss_order_id = stop_loss_order_info['orderId']
            self.logger.debug(
                f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                f'STOPLOSS {str(position_side)} order for {self.symbol.symbol} created\n'
                f'ID: {stop_loss_order_id} StopPrice {str(new_price)}\n')
        except StopTryingSignal:
            stop_loss_order_id = -1
        except Exception as exc:
            raise exc

        self.order_ids['STOP_ORDER'] = stop_loss_order_id
        stop_order_event = self.add_order_event(stop_loss_order_id)

        return stop_order_event, new_price

    async def set_trailing(self, position_side: PositionSide, price: Decimal, quantity: Decimal
                           ) -> typing.Tuple[asyncio.Event, Decimal]:
        sign = 1 if position_side == PositionSide.LONG else -1
        trailing_activation_price = Calculator.round_to_tick(
            price * (1 + self.settings_dump['trail_percent'] / 100 * sign), self.price_precision
        )
        callback_rate = self.settings_dump['trail_SL_distance']
        while True:
            try:
                trailing_order_info = await self.post_order(
                    order_type=OrderType.TRAILING_STOP,
                    callback_rate=callback_rate,
                    side=Side.SELL if position_side == PositionSide.LONG else Side.BUY,
                    position_side=position_side,
                    activation_price=trailing_activation_price,
                    quantity=quantity,
                )
                trailing_order_id = trailing_order_info['orderId']
                ACT_PRICE = trailing_activation_price.quantize(Decimal('0.01'))
                self.logger.debug(
                    f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                    f'TRAIL {str(position_side)} order for {self.symbol.symbol} created\n'
                    f'ID: {trailing_order_id} ActPrice {str(ACT_PRICE)} CbR {str(callback_rate)}%\n')

            except StopTryingSignal:
                trailing_order_id = -1
            except OrderWouldImmediatelyTriggerError:
                self.logger.debug(f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                                  f'Could not set TRAIL {str(position_side)}\n'
                                  f'{self.symbol.symbol} Price: {trailing_activation_price}\n')
                trailing_activation_price = self.last_mark_price + sign * self.price_precision
                continue
            except Exception as exc:
                raise exc

            self.order_ids['TRAILING'] = trailing_order_id
            trailing_order_event = self.add_order_event(trailing_order_id)

            return trailing_order_event, trailing_activation_price

    async def move_stoploss(
            self, price: Decimal, quantity: Decimal, trail_price_event: asyncio.Event, position_side: PositionSide
    ):
        try:
            await trail_price_event.wait()
            async with self.order_lock:
                current_mark_price = self.last_mark_price
                sl0 = self.settings_dump['initial_SL0']
                if sl0:
                    prev_stop_loss_order_id = self.order_ids['STOP_ORDER']
                    await self.cancel_order(prev_stop_loss_order_id)
                    self.logger.debug(
                        f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                        f'STOPLOSS {str(position_side)} order for {self.symbol.symbol} cancelled\n'
                        f'ID: {prev_stop_loss_order_id}\n')
                else:
                    prev_stop_loss_order_id = None

                sign = 1 if position_side == PositionSide.LONG else -1
                sl_stop_price = Calculator.round_to_tick(
                    price * (1 + sign * self.settings_dump['move_init_SL_by'] / 100), self.tick_size)
                new_price = sl_stop_price - sign * self.tick_size
                try:
                    stop_loss_order_info = await self.post_order(
                        order_type=OrderType.STOP_LOSS,
                        stop_price=sl_stop_price,
                        price=new_price,
                        position_side=position_side,
                        side=Side.SELL if position_side == PositionSide.LONG else Side.BUY,
                        quantity=quantity
                    )
                    stop_loss_order_id = stop_loss_order_info['orderId']

                    PREVIOUS_SL_ORDER_ID = prev_stop_loss_order_id if prev_stop_loss_order_id else '--NO SL--'
                    CURRENT_MARK_PRICE = current_mark_price.quantize(Decimal('0.01'))
                    NEW_STOP_PRICE = new_price.quantize(Decimal('0.01'))
                    self.logger.info(
                        f'\nMOVE STOPLOSS ID:{PREVIOUS_SL_ORDER_ID} for {self.symbol.symbol}\n'
                        f'CurrentPrice={str(current_mark_price)}\n'
                        f'NEW STOPLOSS {str(position_side)} order created\n'
                        f'ID: {stop_loss_order_id} On MarketPrice {str(new_price)}\n')
                except StopTryingSignal:
                    stop_loss_order_id = -1

                except Exception as exc:
                    raise exc

                self.order_ids['STOP_ORDER'] = stop_loss_order_id
                stop_order_event = self.add_order_event(stop_loss_order_id)

                return stop_order_event, new_price
        except asyncio.CancelledError as err:
            self.logger.info(
                f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                f'MOVING STOPLOSS {str(position_side)} FOR {self.symbol.symbol} NOT HAPPENED\n'
            )
            raise err
        except Exception as exc:
            raise exc

    async def split_trail(self, price: Decimal, quantity: Decimal, split_trail_event: asyncio.Event,
                          position_side: PositionSide):
        try:
            await split_trail_event.wait()
            async with self.order_lock:
                prev_trail_order_id = self.order_ids['TRAILING']
                await self.cancel_order(prev_trail_order_id)
                current_mark_price = self.last_mark_price



        except asyncio.CancelledError as err:
            self.logger.info(
                f'\n[{datetime.datetime.now()}] {self.symbol.symbol}\n'
                f'\nSPLITTING TRAIL {str(position_side)} FOR {self.symbol.symbol} NOT HAPPENED\n'
            )
            raise err
        except Exception as exc:
            raise exc

    async def process_minute_bar(self, msg) -> bool:
        kline = msg['k']
        closed = kline['x']
        open_p, close_p, high_p, low_p, volume, timestamp = map(Decimal, [
            kline['o'], kline['c'], kline['h'], kline['l'], kline['v'], kline['t']
        ])
        bar = Bar(
            open=open_p,
            close=close_p,
            low=low_p,
            high=high_p,
            volume=volume,
            timestamp=datetime.datetime.fromtimestamp(timestamp // 1000)
        )
        if closed:
            self.history.pop(0)
            self.history.append(bar)
        else:
            self.current_bar = bar

        return closed

    async def handle_bar(self):
        socket = self.bsm.kline_futures_socket(symbol=self.symbol.symbol, interval=TIMEFRAME)
        async with socket:
            while True:
                try:
                    msg = await socket.recv()
                    check_ping(msg, self.logger)
                    await self.process_minute_bar(msg)
                except asyncio.TimeoutError as exc:
                    self.logger.exception(exc)
                    await socket._run_reconnect()
                    continue
                except Exception as exc:
                    if is_stupid_error(exc):
                        await socket._run_reconnect()
                        continue
                    else:
                        raise exc

    async def update_24bars(self):
        await asyncio.sleep(get_seconds_to_next_hour() + 10)
        while True:
            for i in range(RETRY_COUNT):
                try:
                    bars = await self.client.futures_klines(
                        symbol=self.symbol.symbol, interval=KLINE_INTERVAL_1HOUR, limit=25)
                    break
                except (BinanceAPIException, asyncio.TimeoutError) as exc:
                    self.logger.exception(exc)
                    if i == RETRY_COUNT - 1:
                        raise exc
                    await asyncio.sleep(RETRY_TIMEOUT)
                    continue
                except Exception as exc:
                    if is_stupid_error(exc):
                        await asyncio.sleep(RETRY_TIMEOUT)
                        continue
                    else:
                        raise exc
            last_kline = bars[-2]
            self.recent_bars24.pop(0)
            timestamp, open_p, high_p, low_p, close_p, volume = map(Decimal, last_kline[:6])
            bar = Bar(
                open=open_p,
                close=close_p,
                low=low_p,
                high=high_p,
                volume=volume,
                timestamp=datetime.datetime.fromtimestamp(timestamp // 1000),
            )
            self.recent_bars24.append(bar)
            await asyncio.sleep(get_seconds_to_next_hour() + 10)

    async def handle_mark_price(self, data):
        price = Decimal(data['p'])
        self.last_mark_price = price

        # TRIGGER EVENTS
        for (trigger_price, side), event in self.hanging_price_events.items():
            triggered = (
                                side == PositionSide.LONG and self.last_mark_price >= trigger_price
                        ) or (
                                side == PositionSide.SHORT and self.last_mark_price <= trigger_price
                        )
            if triggered:
                async with self.order_lock:
                    event.set()

    def set_triggered(self):
        self.stop_trying = True

    async def handle_order_updates(self, update):
        async with self.order_lock:
            order_id = update['i']
            status = update['X']
            rp = update['rp']
            if order_id in self.hanging_order_events and status == 'FILLED':
                self.hanging_order_events[order_id].set()
                self.set_triggered()
                self.add_order_info(order_id, update)
                self.last_realized_profit = Decimal(rp)
