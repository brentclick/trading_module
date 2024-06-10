from datetime import datetime, timedelta, timezone
from typing import List, Dict

class FuturesOptionsRetriever:
    def __init__(self, api_client):
        self.api_client = api_client

    async def get_expiring_options(self, root: str) -> List[Dict]:
        symbols_data = await self.api_client.get_fo_symbols(root)
        if not symbols_data:
            return []

        one_month_from_now = datetime.now(tz=timezone.utc) + timedelta(days=30)
        expiring_options = [
            symbol for symbol in symbols_data
            if symbol['ExpirationDate'] and datetime.fromisoformat(symbol['ExpirationDate']) <= one_month_from_now
        ]

        return expiring_options

    async def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        quotes = await self.api_client.get_quote_snapshots(symbols)
        return {quote['Symbol']: quote['Last'] for quote in quotes['Quotes']}

    async def get_expiring_options_with_prices(self, root: str) -> List[Dict]:
        expiring_options = await self.get_expiring_options(root)
        if not expiring_options:
            return []

        symbols = [option['Name'] for option in expiring_options]
        prices = await self.get_current_prices(symbols)

        for option in expiring_options:
            option['CurrentPrice'] = prices.get(option['Name'], None)

        return expiring_options
