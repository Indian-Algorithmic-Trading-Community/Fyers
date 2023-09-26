from dataclasses import dataclass
import toml
import asyncio
import pandas as pd


@dataclass
class PatternSettings:
    HANGING_MAN: dict
    SHOOTING_STAR: dict
    SPINNING_TOP: dict
    MARUBOZU: dict
    ENGULFING: dict
    TWEEZER: dict
    MORNING_STAR: dict


class PatternsAnalyzer:
    def __init__(self, data, settings):
        with open(settings, 'r') as f:
            self.settings = PatternSettings(**toml.load(f))
        self.data = data
        self._apply_marubozu = lambda x: x.body_perc > self.settings.MARUBOZU['MATCH']

    async def apply_patterns(self) -> pd.DataFrame:
        df_an = PatternsAnalyzer._apply_candle_props(self.data)
        self._set_candle_patterns(df_an)
        return df_an

    # Private methods prefixed with "_"
    def _apply_hanging_man(self, row) -> bool:
        if row.body_bottom_perc > self.settings.HANGING_MAN['HEIGHT']:
            if row.body_perc < self.settings.HANGING_MAN['BODY']:
                return True
        return False

    def _apply_shooting_star(self, row):
        if row.body_top_perc < self.settings.SHOOTING_STAR['HEIGHT']:
            if row.body_perc < self.settings.HANGING_MAN['BODY']:
                return True
        return False

    def _apply_spinning_top(self, row):
        if row.body_top_perc < self.settings.SPINNING_TOP['MAX']:
            if row.body_bottom_perc > self.settings.SPINNING_TOP['MIN']:
                if row.body_perc < self.settings.HANGING_MAN['BODY']:
                    return True
        return False

    def _apply_engulfing(self, row):
        if row.direction != row.direction_prev:
            if row.body_size > row.body_size_prev * self.settings.ENGULFING['FACTOR']:
                return True
        return False

    def _apply_tweezer_top(self, row):
        if abs(row.body_size_change) < self.settings.TWEEZER['BODY']:
            if row.direction == -1 and row.direction != row.direction_prev:
                if abs(row.low_change) < self.settings.TWEEZER['HL'] and abs(row.high_change) < self.settings.TWEEZER['HL']:
                    if row.body_top_perc < self.settings.TWEEZER['TOP_BODY']:
                        return True
        return False

    def _apply_tweezer_bottom(self, row):
        if abs(row.body_size_change) < self.settings.TWEEZER['BODY']:
            if row.direction == 1 and row.direction != row.direction_prev:
                if abs(row.low_change) < self.settings.TWEEZER['HL'] and abs(row.high_change) < self.settings.TWEEZER['HL']:
                    if row.body_bottom_perc > self.settings.TWEEZER['BOTTOM_BODY']:
                        return True
        return False

    def _apply_morning_star(self, row, direction=1):
        if row.body_perc_prev_2 > self.settings.MORNING_STAR['PREV2_BODY']:
            if row.body_perc_prev < self.settings.MORNING_STAR['PREV_BODY']:
                if row.direction == direction and row.direction_prev_2 != direction:
                    if direction == 1:
                        if row.close > row.mid_point_prev_2:
                            return True
                    else:
                        if row.close < row.mid_point_prev_2:
                            return True
        return False

    @staticmethod
    def _apply_candle_props(df: pd.DataFrame) -> pd.DataFrame:
        direction = df.close - df.open
        body_size = abs(direction)
        direction = [1 if x >= 0 else -1 for x in direction]
        full_range = df.high - df.low
        body_perc = (body_size / full_range) * 100
        body_lower = df[['close', 'open']].min(axis=1)
        body_upper = df[['close', 'open']].max(axis=1)
        body_bottom_perc = ((body_lower - df.low) / full_range) * 100
        body_top_perc = ((body_upper - df.low) / full_range) * 100

        mid_point = full_range / 2 + df.low

        low_change = df.low.pct_change() * 100
        high_change = df.high.pct_change() * 100
        body_size_change = body_size.pct_change() * 100

        df['body_lower'] = body_lower
        df['body_upper'] = body_upper
        df['body_bottom_perc'] = body_bottom_perc
        df['body_top_perc'] = body_top_perc
        df['body_perc'] = body_perc
        df['direction'] = direction
        df['body_size'] = body_size
        df['low_change'] = low_change
        df['high_change'] = high_change
        df['body_size_change'] = body_size_change
        df['mid_point'] = mid_point
        df['mid_point_prev_2'] = mid_point.shift(2)
        df['body_size_prev'] = df.body_size.shift(1)
        df['direction_prev'] = df.direction.shift(1)
        df['direction_prev_2'] = df.direction.shift(2)
        df['body_perc_prev'] = df.body_perc.shift(1)
        df['body_perc_prev_2'] = df.body_perc.shift(2)

        return df

    def _set_candle_patterns(self, df: pd.DataFrame):
        df['HANGING_MAN'] = df.apply(self._apply_hanging_man, axis=1)
        df['SHOOTING_STAR'] = df.apply(self._apply_shooting_star, axis=1)
        df['SPINNING_TOP'] = df.apply(self._apply_spinning_top, axis=1)
        df['MARUBOZU'] = df.apply(self._apply_marubozu, axis=1)
        df['ENGULFING'] = df.apply(self._apply_engulfing, axis=1)
        df['TWEEZER_TOP'] = df.apply(self._apply_tweezer_top, axis=1)
        df['TWEEZER_BOTTOM'] = df.apply(self._apply_tweezer_bottom, axis=1)
        df['MORNING_STAR'] = df.apply(self._apply_morning_star, axis=1)
        df['EVENING_STAR'] = df.apply(self._apply_morning_star, axis=1, direction=-1)


# Example usage
# from infrastructure.get_data import GetData
#
# gd = GetData()
# candles = asyncio.run(gd.get_index_candles("NIFTY50"))
# analyzer = PatternsAnalyzer(candles, settings="../pattern_settings.toml")
# _data = asyncio.run(analyzer.apply_patterns())
# print(_data)
