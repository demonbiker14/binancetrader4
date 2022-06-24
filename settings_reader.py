import configparser
import logging
from configparser import ConfigParser
from decimal import Decimal

logger = logging.getLogger('.')


def read_settings():
    settings = ConfigParser()
    settings.read('settings.ini')
    settings = {
        'min_balance': Decimal(settings.get('DEFAULT', 'min_balance', fallback='0.0')),
        'E_leverage': Decimal(settings.get('DEFAULT', 'E_leverage', fallback='1.0')),
        'max_leverage': int(settings.get('DEFAULT', 'max_leverage')),
        'min_leverage': int(settings.get('DEFAULT', 'min_leverage')),
        'blacklist': settings.get('DEFAULT', 'blacklist', fallback=[]),
        'doji': int(settings.get('DEFAULT', 'doji', fallback=0)),
        'spike': Decimal(settings.get('DEFAULT', 'spike', fallback='0.0')),
        'min_vol_usdt': Decimal(settings.get('DEFAULT', 'min_Vol_usdt', fallback='0.0')),

        'pause_N_minute': int(settings.get('DEFAULT', 'pause_N_minute')),
        'pause_Invert': settings.get('DEFAULT', 'pause_Invert'),

        'lot_D_buy': Decimal(settings.get('DEFAULT', 'lot_D_buy', fallback='0.0')),
        'lot_D_sell': Decimal(settings.get('DEFAULT', 'lot_D_sell', fallback='0.0')),
        'minute_K_long': int(settings.get('DEFAULT', 'minute_K_long')),
        'minute_K_short': int(settings.get('DEFAULT', 'minute_K_short')),
        'change_J_long': Decimal(settings.get('DEFAULT', 'change_J_long')),
        'change_J_short': Decimal(settings.get('DEFAULT', 'change_J_short')),
        'minute_W_long': int(settings.get('DEFAULT', 'minute_W_long')),
        'minute_W_short': int(settings.get('DEFAULT', 'minute_W_short')),

        'initial_SL0': Decimal(settings.get('DEFAULT', 'initial_SL0')),
        'trail_percent': Decimal(settings.get('DEFAULT', 'trail_percent')),
        'trail_SL_distance': Decimal(settings.get('DEFAULT', 'trail_SL_distance')),
        'split_SLH_percent': Decimal(settings.get('DEFAULT', 'split_SLH_percent')),
        'move_init_SL_by': Decimal(settings.get('DEFAULT', 'move_init_SL_by'))
    }
    return settings


class Settings:
    data: dict

    def __init__(self):
        self.data = {}

    def fetch(self):
        self.data = read_settings()

    def get_dump(self):
        prev_data = self.data.copy()
        try:
            self.fetch()
        except (configparser.NoOptionError, configparser.ParsingError) as err:
            logger.exception(err)
            self.data = prev_data
        return self.data.copy()
