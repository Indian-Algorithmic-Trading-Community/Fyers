from login.login import BrokerHandler
import asyncio
from datetime import datetime, timedelta
import pandas as pd


class GetData:
    def __init__(self):
        self.handler = BrokerHandler(credentials_file="../credentials.toml", access_token="../access_token.txt")

    @staticmethod
    def get_epoch(datetimeObj=None):
        """
        Convert a datetime object to EPOCH format.
        :param datetimeObj: 
        :return: EPOCH format date
        """""
        if datetimeObj is None:
            datetimeObj = datetime.now()
        epochSeconds = datetime.timestamp(datetimeObj)
        return int(epochSeconds)

    @staticmethod
    def process_candles(response, data_frame: bool = True):
        """
        Process candles from the response.
        :param response:
        :param data_frame:
        :return:
        """
        if data_frame:
            df = pd.DataFrame(response['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['date'] = (pd.to_datetime(df['date'], unit='s')
                          .dt.tz_localize('utc')
                          .dt.tz_convert('Asia/Kolkata')
                          .dt.tz_localize(None))
            return df.set_index('date')
        else:
            return response

    async def get_stock_candles(self,
                                symbol: str,
                                interval: int = 5,
                                duration: int = 100,
                                exchange: str = "NSE",
                                segment: str = "EQ",
                                continuous: bool = True,
                                data_frame: bool = True,
                                **kwargs
                                ) -> pd.DataFrame | dict | None:
        """
        Get candles for a given symbol either cash or Future.
        :param symbol: Symbol name. Ex- "SBIN" NIFTY
        :param interval: Resolution of candles. Ex- 5 for 5 minutes, 15 for 15 minutes, 60 for 1 hour, 240 for 4 hours.
        :param duration: Number of days candles to be fetched.
        :param exchange: Stock exchange of the symbol. Ex- NSE, BSE, MCX, CDS
        :param segment: Market segment of the symbol. Ex- "EQ" for equity, expiry for commodity, CE/PE for options.
        :param continuous: If True, return continuous future data in the specified duration.
        :param data_frame: If True, return a Pandas Dataframe. If False, return a Dictionary of Candles.
        :param kwargs:
        :return: Pandas Dataframe or Dictionary of Candles.
        """
        _symbol = {
            "EQ": f"{exchange}:{symbol}-{segment}",
            "FUT": f"{exchange}:{symbol}{datetime.now().strftime('%y%b').upper()}FUT",
        }
        fyers = await self.handler.get_instance()
        _data = {
            "symbol": _symbol[segment],
            "resolution": str(interval),
            "date_format": "0",
            "range_from": GetData.get_epoch(datetime.now() - timedelta(days=duration)),
            "range_to": GetData.get_epoch(),
            "cont_flag": "1" if continuous else "0",
        }
        response = await fyers.history(data=_data)
        respo = self.process_candles(response, data_frame=data_frame)
        return respo

    async def get_index_candles(self,
                                symbol: str,
                                interval: int = 5,
                                duration: int = 100,
                                exchange: str = "NSE",
                                data_frame: bool = True,
                                **kwargs
                                ) -> pd.DataFrame | dict | None:
        """
        Get candles for a given Index.
        :param symbol: Symbol name. Ex- "NIFTY"
        :param interval: Resolution of candles. Ex- 5 for 5 minutes, 15 for 15 minutes, 60 for 1 hour, 240 for 4 hours.
        :param duration: Number of days candles to be fetched.
        :param exchange: Stock exchange of the symbol. Ex- NSE, BSE, MCX
        :param data_frame: If True, return a Pandas Dataframe. If False, return a Dictionary of Candles.
        :param kwargs:
        :return: Pandas Dataframe or Dictionary of Candles.
        """
        fyers = await self.handler.get_instance()
        _data = {
            "symbol": f"{exchange}:{symbol}-INDEX",
            "resolution": str(interval),
            "date_format": "0",
            "range_from": GetData.get_epoch(datetime.now() - timedelta(days=duration)),
            "range_to": GetData.get_epoch(),
            "cont_flag": "0",
        }
        response = await fyers.history(data=_data)
        respo = self.process_candles(response, data_frame=data_frame)
        return respo

    async def get_quotes(self,
                         tickers: list = None,
                         segment: str = "EQ",
                         data_frame: bool = True,
                         **kwargs
                         ) -> pd.DataFrame | dict | None:
        """
        Get quotes for a given symbol.
        :param tickers: List of symbols ["SBIN", "PNB"].
        :param segment: EQ for equity and FUT is present month future contract.
        :param data_frame: If True, return a Pandas Dataframe. If False, return a Dictionary of Candles.
        :param kwargs:
        :return: Pandas Dataframe or Dictionary of quote of all tickers provided.
        """
        if segment == "EQ":
            _segment = "-EQ"
        else:
            _segment = f"{datetime.now().strftime('%y%b').upper()}FUT"
        formatted_tickers = ",".join([f"NSE:{tick}{_segment}" for tick in tickers])
        fyers = await self.handler.get_instance()
        _data = await fyers.quotes(data={"symbols": formatted_tickers})

        if data_frame:
            processed_data = []
            for entry in _data['d']:
                values = entry['v']
                processed_data.append(
                    {
                        "symbols": entry['n'],
                        "ask": values.get('ask', None),
                        "bid": values.get('bid', None),
                        "spread": values.get('spread', None),
                        "ltp": values.get('lp', None),
                        "change": values.get('ch', None),
                        "change_per": values.get('chp', None),
                        "previous_close": values.get('prev_close_price', None),
                        "open": values.get('open_price', None),
                        "day_high": values.get('high_price', None),
                        "day_low": values.get('low_price', None),
                        "volume": values.get('volume', None),
                        "time": values.get('tt', None),
                    }
                )
            return pd.DataFrame(processed_data)
        else:
            return _data
