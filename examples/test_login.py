from ..login.login import LoginHandle
import asyncio
from fyers_apiv3 import fyersModel
import toml

with open("../credentials.toml", 'r') as f:
    config = toml.load(f)
client_id = config['FYERS']['APP_ID']

bh = LoginHandle()

async def get_profile():
    valid_token = await bh.get_valid_token()
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=True, 
                                  token=valid_token, log_path="../logs")

    response = await fyers.get_profile()
    print(response)
    
if __name__ == "__main__":
    print(__package__)
    asyncio.run(get_profile())
