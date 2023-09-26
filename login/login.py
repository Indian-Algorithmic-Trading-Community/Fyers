import toml
from fyers_apiv3 import fyersModel


class BrokerHandler:
    def __init__(self, credentials_file, access_token, log_path="../logs"):
        with open(credentials_file, 'r') as f:
            self.config = toml.load(f)
        self.client_id = self.config['FYERS']['APP_ID']
        self.access_token = access_token
        self.log_path = log_path
        self.fyers_instance = None

    async def _initialize_fyers_instance(self):
        self.fyers_instance = fyersModel.FyersModel(client_id=self.client_id,
                                                    is_async=True, token=self.access_token,
                                                    log_path=self.log_path)

    async def get_instance(self):
        if self.fyers_instance is None:
            await self._initialize_fyers_instance()
        return self.fyers_instance
