from login.login import BrokerHandler
import asyncio


class FyersTest:
    def __init__(self):
        self.handler = BrokerHandler(credentials_file="../credentials.toml", access_token="../access_token.txt")

    async def get_profile(self):
        fyers = await self.handler.get_instance()
        response = await fyers.get_profile()
        return response

    async def get_funds(self):
        fyers = await self.handler.get_instance()
        response = await fyers.funds()
        return response


if __name__ == "__main__":
    handler = FyersTest()
    asyncio.run(handler.get_funds())
