#trading_module/__main__.py

import asyncio
from trading_module import AuthClient, TradeStationAPIClient, DatabaseClient, StreamClient, FuturesOptionsRetriever
import trading_module.config as config

async def main():
    auth_client = AuthClient(
        client_id=config.client_id,
        client_secret=config.client_secret,
        refresh_token=config.refresh_token,
        redirect_uri=config.redirect_uri,
        base_url=config.base_url
    )
    
    api_client = TradeStationAPIClient(auth_client)
    db_client = DatabaseClient()
    futures_retriever = FuturesOptionsRetriever(api_client)
    stream_client = StreamClient(auth_client)

    # Example Usage
    accounts_data = await api_client.get_accounts_data(sim=True)
    print(accounts_data)

    expiring_options = await futures_retriever.get_expiring_options("ES")
    db_client.upload_symbols_to_db(expiring_options)

    await stream_client.stream_quotes("AAPL")

if __name__ == "__main__":
    asyncio.run(main())
