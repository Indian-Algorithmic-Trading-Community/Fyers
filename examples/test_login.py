from login.login import LoginHandle
import asyncio
from fyers_apiv3 import fyersModel
import toml


class FyersTest:
    def __init__(self, credentials_file, log_path="../logs"):
        with open(credentials_file, 'r') as f:
            self.config = toml.load(f)
        self.client_id = self.config['FYERS']['APP_ID']
        self.bh = LoginHandle(credentials_file=credentials_file)
        self.log_path = log_path
        self.valid_token = None
        self.fyers_instance = None

    async def _initialize_fyers_instance(self):
        self.valid_token = await self.bh.get_valid_token()
        self.fyers_instance = fyersModel.FyersModel(client_id=self.client_id,
                                                    is_async=True, token=self.valid_token,
                                                    log_path=self.log_path)

    async def _get_fyers_instance(self):
        if self.fyers_instance is None:
            await self._initialize_fyers_instance()
        return self.fyers_instance

    async def get_profile(self):
        fyers = await self._get_fyers_instance()
        response = await fyers.get_profile()
        return response

    async def get_funds(self):
        fyers = await self._get_fyers_instance()
        response = await fyers.funds()
        return response


if __name__ == "__main__":
    handler = FyersTest(credentials_file="../credentials.toml")
    asyncio.run(handler.get_funds())
