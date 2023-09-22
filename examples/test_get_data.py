from infrastructure.get_data import GetData
import asyncio


class FyersTest:
    def __init__(self):
        self._handler = GetData()

    async def test_quotes(self):
        tickers = ["SBIN", "PNB"]
        futures_data = await self._handler.get_quotes(tickers=tickers, segment="FUT")
        equity_data = await self._handler.get_quotes(tickers=tickers)
        print(f"Futures Quote >>>>>>>>> \n{futures_data}\n")
        print(f"Equity Quote >>>>>>>>> \n{equity_data}\n")

    async def test_stock_candles(self):
        futures_candle = await self._handler.get_stock_candles(symbol="SBIN", segment="FUT")
        equity_candle = await self._handler.get_stock_candles(symbol="SBIN")
        print(f"Futures Candles >>>>>>>>> \n{futures_candle}\n")
        print(f"Equity Candles >>>>>>>>> \n{equity_candle}\n")

    async def test_index_candles(self):
        index_candle = await self._handler.get_index_candles(symbol="NIFTY50")
        print(f"Index Candles >>>>>>>>> \n{index_candle}\n")


async def main():
    _test = FyersTest()
    # asyncio.run(_test.test_quotes())
    t1 = asyncio.create_task(_test.test_quotes())
    t2 = asyncio.create_task(_test.test_stock_candles())
    t3 = asyncio.create_task(_test.test_index_candles())
    await asyncio.gather(t1, t2, t3)

if __name__ == "__main__":
    asyncio.run(main())
