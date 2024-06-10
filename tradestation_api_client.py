# trading_module/tradestation_api_client.py

import httpx
from datetime import datetime, timedelta, timezone
import logging
from typing import Union, List, Dict

class TradestationAPIClient:
    def __init__(self, auth_client):
        self.auth_client = auth_client

    async def _make_request(self, endpoint: str, params: Dict[str, str] = None) -> Dict:
        access_token = self.auth_client.get_access_token()
        url = f"{self.auth_client.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {access_token}"}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

    def _parse_datetime(self, date_str: str) -> str:
        if date_str and date_str.startswith("/Date("):
            timestamp = int(date_str[6:-2])
            return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat()
        return date_str

    async def get_accounts_data(self, sim=False) -> List[Dict]:
        endpoint = "v3/brokerage/accounts" if not sim else "v3/brokerage/sim/accounts"
        data = await self._make_request(endpoint)
        return [self._format_account_data(account) for account in data.get('Accounts', [])]

    def _format_account_data(self, account: Dict) -> Dict:
        formatted_account = {
            "AccountID": account.get("AccountID"),
            "Currency": account.get("Currency"),
            "Status": account.get("Status"),
            "AccountType": account.get("AccountType"),
            **account.get("AccountDetail", {})
        }
        return formatted_account

    async def get_fo_symbols(self, root: str) -> List[Dict]:
        endpoint = f"v2/data/symbols/search/C=FO&R={root}&Stk=100&Exd=100"
        data = await self._make_request(endpoint)
        return [self._format_option_detail(item) for item in data]

    def _format_option_detail(self, item: Dict) -> Dict:
        return {
            "Name": item.get("Name", ""),
            "Description": item.get("Description", ""),
            "Exchange": item.get("Exchange", ""),
            "ExchangeID": item.get("ExchangeID", ""),
            "Category": item.get("Category", ""),
            "Country": item.get("Country", ""),
            "Root": item.get("Root", ""),
            "OptionType": item.get("OptionType", ""),
            "FutureType": item.get("FutureType", ""),
            "ExpirationDate": self._parse_datetime(item.get("ExpirationDate", "")),
            "ExpirationType": item.get("ExpirationType", ""),
            "StrikePrice": item.get("StrikePrice", ""),
            "Currency": item.get("Currency", ""),
            "PointValue": item.get("PointValue", ""),
            "MinMove": item.get("MinMove", ""),
            "DisplayType": item.get("DisplayType", ""),
            "Underlying": item.get("Underlying", ""),
            "LotSize": item.get("LotSize", ""),
            "IndustryCode": item.get("IndustryCode", ""),
            "IndustryName": item.get("IndustryName", ""),
            "SectorName": item.get("SectorName", ""),
            "SectionClassCode": item.get("SectionClassCode", ""),
            "SectionClassName": item.get("SectionClassName", ""),
            "IsPreferredExchange": item.get("IsPreferredExchange", False),
            "Error": item.get("Error", None)
        }

    async def get_positions(self, account_id: str) -> Union[Dict, None]:
        is_sim = "SIM" in account_id.upper()
        endpoint = f"v3/brokerage/{'sim/' if is_sim else ''}accounts/{account_id}/positions"
        return await self._make_request(endpoint)

    async def get_quote_snapshots(self, symbols: List[str]) -> Dict:
        endpoint = f"v3/marketdata/quotes/{','.join(symbols)}"
        return await self._make_request(endpoint)

    async def get_bars(self, symbol: str, interval: int = 1, unit: str = "Minute", barsback: int = None, firstdate: Union[str, datetime] = None, lastdate: Union[str, datetime] = datetime.now()) -> Union[Dict, None]:
        endpoint = f"marketdata/barcharts/{symbol}"
        params = {
            "interval": interval,
            "unit": unit,
            "sessiontemplate": 'Default',
            "lastdate": lastdate.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        if firstdate:
            params["firstdate"] = firstdate.strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(firstdate, datetime) else firstdate
        else:
            params["barsback"] = barsback

        data = await self._make_request(endpoint, params)
        return self._format_bar_data(data)

    def _format_bar_data(self, data: Dict) -> Dict:
        return {"Bars": [self._format_single_bar(bar) for bar in data.get("Bars", [])]}

    def _format_single_bar(self, bar: Dict) -> Dict:
        return {
            "High": bar.get("High", ""),
            "Low": bar.get("Low", ""),
            "Open": bar.get("Open", ""),
            "Close": bar.get("Close", ""),
            "Timestamp": bar.get("TimeStamp", ""),
            "TotalVolume": bar.get("TotalVolume", ""),
            "DownTicks": bar.get("DownTicks", ""),
            "DownVolume": bar.get("DownVolume", ""),
            "OpenInterest": bar.get("OpenInterest", ""),
            "TotalTicks": bar.get("TotalTicks", ""),
            "UnchangedTicks": bar.get("UnchangedTicks", ""),
            "UnchangedVolume": bar.get("UnchangedVolume", ""),
            "UpTicks": bar.get("UpTicks", ""),
            "UpVolume": bar.get("UpVolume", "")
        }

    async def place_order(self, account_id: str, symbol: str, quantity: int, trade_action: str, order_type: str, price: float = None, trailing_stop: float = None, trailing_stop_type: str = None) -> Union[Dict, None]:
        is_sim = "SIM" in account_id.upper()
        endpoint = f"v3/orderexecution/orders"
        url = f"https://{'sim-' if is_sim else ''}api.tradestation.com/{endpoint}"
        headers = {"Authorization": f"Bearer {self.auth_client.get_access_token()}", "Content-Type": "application/json"}

        payload = {
            'AccountID': account_id,
            'Symbol': symbol,
            'Quantity': str(quantity),
            'TradeAction': trade_action.upper(),
            'OrderType': order_type.capitalize(),
            'TimeInForce': {'Duration': 'DAY'},
            'Route': 'Intelligent'
        }

        if price:
            payload['Price'] = price
        if trailing_stop:
            payload['AdvancedOrder'] = {'TrailingStop': trailing_stop, 'TrailingStopType': trailing_stop_type, 'TrailingStopDuration': 'DAY'}

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()