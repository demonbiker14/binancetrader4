import datetime
from decimal import Decimal

import typing

from config import LOG_FOLDER, BASE_ASSET
from constants import PositionSide, Symbol
from settings_reader import Settings


class DealInfo:
    log_file_name: str
    log_file_created: bool

    position_side: PositionSide

    enter_price: Decimal
    change_price: Decimal
    trail_price: Decimal
    stoploss_price: Decimal
    quantity: Decimal
    base_asset_quantity: Decimal
    change: Decimal
    settings_dump: dict
    leverage: int

    trail_order_id: int
    stoploss_order_id: int

    enter_time: datetime.datetime
    close_time: datetime.datetime

    close_price: Decimal
    realized_profit: Decimal
    closed_by: str

    closed = False

    def __init__(self, symbol: Symbol, position_side: PositionSide):
        self.closed = False
        self.symbol = symbol
        self.position_side = position_side
        self.log_file_created = False

    def create_log_file(self):
        dt_str = self.enter_time.strftime('%Y-%M-%d-%H-%m-%S')
        self.log_file_name = f'{LOG_FOLDER}/{dt_str}-{self.symbol.symbol}.log'
        with open(self.log_file_name, mode='w'):
            pass
        self.log_file_created = True

    def open_deal(
            self,
            enter_price: Decimal,
            trail_price: Decimal,
            stoploss_price: Decimal,

            quantity: Decimal,
            base_asset_quantity: Decimal,
            change: Decimal,
            change_price: Decimal,

            settings_dump: dict,


            leverage: int,

            trail_order_id: int,
            stoploss_order_id: typing.Optional[int],

            enter_time: datetime.datetime,
    ):
        self.enter_price = enter_price
        self.trail_price = trail_price
        self.stoploss_price = stoploss_price

        self.quantity = quantity
        self.base_asset_quantity = base_asset_quantity
        self.leverage = leverage

        self.change = change
        self.change_price = change_price
        self.settings_dump = settings_dump

        self.trail_order_id = trail_order_id
        self.stoploss_order_id = stoploss_order_id

        self.enter_time = enter_time

        KK = settings_dump['minute_K_long' if self.position_side == PositionSide.LONG else 'minute_K_short']
        JJ = settings_dump['change_J_long' if self.position_side == PositionSide.LONG else 'change_J_short']
        WW = settings_dump['minute_W_long' if self.position_side == PositionSide.LONG else 'minute_W_short']
        CHANGE = self.change.quantize(Decimal('0.01'))
        CALCULATION_CURRENT_PRICE = self.change_price.quantize(Decimal('0.01'))
        BASE_QUANTITY = base_asset_quantity.quantize(Decimal('0.01'))
        TR0 = self.settings_dump['trail_percent']
        TR_DISTANCE = self.settings_dump['trail_SL_distance']
        sl0 = self.settings_dump['initial_SL0']

        self.log(
            f'========  {str(enter_time)} =======\n'
            f'Entered {str(self.position_side)}: {self.symbol.symbol} (x{leverage}) {str(BASE_QUANTITY)} {BASE_ASSET}\n'
            f'Change {str(CHANGE)}% (J={str(JJ)}), K={str(KK)}, W={str(WW)}\n'
            f'Current price: {CALCULATION_CURRENT_PRICE}\n'
            f'AVG IN: {str(self.enter_price)}\n'
            f'SL0: {str(stoploss_price)} (-{str(sl0)}%)\n'
            f'TR0: {str(self.trail_price)} (+{str(TR0)}%), distance {str(TR_DISTANCE)}%\n'
        )

    def close_deal(
            self,
            close_price: Decimal,
            realized_profit: Decimal,
            closed_by: str,
            close_time: datetime.datetime
    ):
        self.close_price = close_price
        self.realized_profit = realized_profit
        self.closed_by = closed_by
        self.closed = True

        self.close_time = close_time

    def log(self, msg):
        if not self.log_file_created:
            self.create_log_file()
        with open(self.log_file_name, mode='a') as f:
            f.write(f'{msg}\n')

    def write_to_excel(self):
        pass

    async def notify_telegram(self):
        pass


if __name__ == '__main__':
    symbol = Symbol('BTC', 'USDT', 'BTCUSDT')
    settings_reader = Settings()
    dump = settings_reader.get_dump()
    deal = DealInfo(
        symbol, PositionSide.LONG
    )
    deal.open_deal(
        enter_price=Decimal('40000'),
        change_price=Decimal('40100'),
        trail_price=Decimal('41000'),
        stoploss_price=Decimal('39000'),
        quantity=Decimal('0.1'),
        base_asset_quantity=Decimal('30'),
        change=Decimal('0.5'),
        settings_dump=dump,
        leverage=2,
        trail_order_id=-2,
        stoploss_order_id=-3,
        enter_time=datetime.datetime.now() - datetime.timedelta(minutes=1)
    )