import math
from decimal import Decimal
from typing import List, Optional

from constants import Bar, PositionSide


class Calculator:
    CHECK_PRICE: Optional[Decimal]
    WW_result: bool
    first_bar: Optional[Bar]
    change: Optional[bool]
    spike: Optional[bool]
    percent_difference: Optional[Decimal]

    def __init__(self, position_side: PositionSide):
        self.position_side = position_side
        self.WW_result = False
        self.CHECK_PRICE = None

    def clear(self):
        self.first_bar = None
        self.change = None
        self.spike = None
        self.percent_difference = None
        self.WW_result = False
        self.CHECK_PRICE = None

    @classmethod
    def get_spike(cls, bar: Bar) -> Decimal:
        if bar.open == bar.close:
            return Decimal('Infinity')
        spike = abs((bar.high - bar.low) / (bar.open - bar.close))
        return spike

    @classmethod
    def volume24(cls, bars: List[Bar]) -> Decimal:
        volumes = [bar.close * bar.volume for bar in bars]
        return sum(volumes)

    @classmethod
    def get_doji(cls, bars: List[Bar], doji: int) -> bool:
        if not doji:
            return False
        bars = bars[-doji:]
        # maybe not strong equality

        first_equalities = [bar.open == bar.close for bar in bars]
        first = False
        for i in range(doji - 1):
            if first_equalities[i] and first_equalities[i + 1]:
                first = True
                break

        if first:
            return True

        # second
        second = False
        if doji >= 3:
            for i in range(doji - 2):
                if bars[i].close == bars[i + 1].close and \
                        bars[i + 1].close == bars[i + 2].close:
                    second = True
                    break
        return first or second

    def get_change(
            self,
            bars: List[Bar],
            KK: int,
            JJ: Decimal,
            WW: int,
            SPIKE: Decimal,
            current_price: Decimal,
    ) -> bool:
        self.first_bar = bars[-KK]
        self.CHECK_PRICE = current_price

        # CLEARING SPIKES
        self.spike = True
        bars = (list(filter(
            lambda bar: type(self).get_spike(bar) <= SPIKE, bars[:-1]
        )) + [bars[-1]]) if SPIKE else bars

        if not bars:
            self.change = False
            self.spike = False
            self.percent_difference = Decimal('0.0')
            return False

        # COUNT THE PERCENT CHANGE
        if self.position_side == PositionSide.LONG:
            self.percent_difference = (current_price / self.first_bar.low - 1) * 100
        else:
            self.percent_difference = (current_price / self.first_bar.high - 1) * 100

        JJ_result = abs(self.percent_difference) >= JJ and (
            self.percent_difference >= 0 if self.position_side == PositionSide.LONG else self.percent_difference <= 0
        )

        bars = bars[-WW:]
        # FIX THAT CURRENT BAR TOO
        self.WW_result = all(map(lambda bar: bar.close >= bar.open, bars)) \
            if self.position_side == PositionSide.LONG else all(map(lambda bar: bar.close <= bar.open, bars))

        self.change = JJ_result and self.WW_result
        return self.change

    @classmethod
    def get_leverage(
            cls, E_leverage: Decimal, balance: Decimal, min_leverage: int, max_leverage: int
    ) -> int:
        leverage = math.ceil(balance / E_leverage)
        leverage = min(max_leverage, max(min_leverage, leverage))
        return leverage

    @classmethod
    def get_quantity(
            cls,
            balance: Decimal,
            lot_D: Decimal,
            tick: Decimal,
            price: Decimal,
            leverage: int,
            quantity_precision: Decimal,
    ) -> Decimal:
        # PRECISE MINIMUM ORDER SIZE
        quantity = (balance * (lot_D / 100))
        quantity = math.ceil(quantity / tick) * tick
        quantity *= leverage
        quantity /= price
        quantity = math.ceil(quantity / quantity_precision) * quantity_precision
        return quantity

    @classmethod
    def round_to_tick(cls, price: Decimal, tick: Decimal):
        if tick == Decimal('0.0'):
            return price
        price = math.ceil(price / tick) * tick
        return price
