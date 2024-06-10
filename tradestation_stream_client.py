#trading_module/tradestation_stream_client.py

import httpx
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

# Configure logging to send info messages to a file
logging.basicConfig(level=logging.INFO, filename='stream_client.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

class TradestationStreamClient:
    def __init__(self, auth_client):
        self.auth_client = auth_client

    async def stream_bars(self, symbol: str, interval: str = "1", unit: str = "Minute", barsback: str = None, sessiontemplate: str = "Default"):
        await self._stream_data(symbol, interval, unit, barsback, sessiontemplate, self._format_streaming_bars, 'marketdata/stream/barcharts')

    async def stream_positions(self):
        await self._stream_data(None, None, None, None, None, self._format_streaming_positions, 'accounts/stream/positions')

    async def stream_orders(self):
        await self._stream_data(None, None, None, None, None, self._format_streaming_orders, 'accounts/stream/orders')

    async def stream_option_quotes(self, symbol: str):
        await self._stream_data(symbol, None, None, None, None, self._format_streaming_option_quotes, 'marketdata/stream/optionquotes')

    async def stream_quotes(self, symbol: str):
        await self._stream_data(symbol, None, None, None, None, self._format_streaming_quotes, 'marketdata/stream/quotes')

    async def stream_option_chains(self, symbol: str):
        await self._stream_data(symbol, None, None, None, None, self._format_streaming_option_chains, 'marketdata/stream/optionchains')

    async def stream_market_depth(self, symbol: str):
        await self._stream_data(symbol, None, None, None, None, self._format_streaming_market_depth, 'marketdata/stream/marketdepth')

    async def stream_aggregate_market_depth(self, symbol: str):
        await self._stream_data(symbol, None, None, None, None, self._format_streaming_aggregate_market_depth, 'marketdata/stream/aggregatemarketdepth')

    async def _stream_data(self, symbol: str, interval: str, unit: str, barsback: str, sessiontemplate: str, formatter, endpoint: str):
        while True:
            try:
                access_token = await self.auth_client.get_access_token()
                url = f'https://api.tradestation.com/v3/{endpoint}'
                if symbol:
                    url += f'/{symbol}'
                
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Transfer-Encoding': 'chunked',
                    'Content-Type': 'application/vnd.tradestation.streams.v2+json'
                }

                params = {}
                if interval:
                    params['interval'] = interval
                if unit:
                    params['unit'] = unit
                if sessiontemplate:
                    params['sessiontemplate'] = sessiontemplate
                if barsback:
                    params['barsback'] = barsback

                async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                    async with client.stream("GET", url, headers=headers, params=params) as response:
                        if response.status_code == 401:
                            logging.warning("Received 401 Unauthorized. Refreshing access token.")
                            await self.auth_client._refresh_access_token()
                            continue
                        elif response.status_code != 200:
                            logging.error(f"Failed to connect to stream: {response.status_code} - {response.text}")
                            break

                        logging.info("Connected to the stream.")
                        async for chunk in response.aiter_text():
                            if chunk:
                                processed = False
                                for line in chunk.splitlines():
                                    if line.strip() and not processed:
                                        try:
                                            data = json.loads(line)
                                            if 'Heartbeat' in data:
                                                logging.info(f"Heartbeat received: {data['Heartbeat']}, Timestamp: {data['Timestamp']}")
                                            else:
                                                if 'Error' in data:
                                                    logging.error(f"Error received: {data['Message']}")
                                                    raise Exception(data['Message'])
                                                formatted_data = formatter(data)
                                                logging.info(f"Formatted Data: {formatted_data}")
                                                processed = True
                                                print (formatted_data)
                                        except json.JSONDecodeError:
                                            logging.warning(f"Received non-JSON data: {line}")
            except httpx.ReadTimeout:
                logging.warning("Read timeout: The request took longer than expected.")
            except httpx.RequestError as e:
                logging.error(f"Request error occurred: {e}")
            except Exception as e:
                logging.error(f"An error occurred: {e}")

            logging.info("Reconnecting to the stream in 10 seconds...")
            await asyncio.sleep(10)

    def _format_streaming_bars(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "High": float(data.get("High", 0)),
            "Low": float(data.get("Low", 0)),
            "Open": float(data.get("Open", 0)),
            "Close": float(data.get("Close", 0)),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None,
            "TotalVolume": int(data.get("TotalVolume", 0)),
            "DownTicks": int(data.get("DownTicks", 0)),
            "DownVolume": int(data.get("DownVolume", 0)),
            "OpenInterest": int(data.get("OpenInterest", 0)),
            "IsRealtime": data.get("IsRealtime", False),
            "IsEndOfHistory": data.get("IsEndOfHistory", False),
            "TotalTicks": int(data.get("TotalTicks", 0)),
            "UnchangedTicks": int(data.get("UnchangedTicks", 0)),
            "UnchangedVolume": int(data.get("UnchangedVolume", 0)),
            "UpTicks": int(data.get("UpTicks", 0)),
            "UpVolume": int(data.get("UpVolume", 0)),
            "Epoch": int(data.get("Epoch", 0)),
            "BarStatus": data.get("BarStatus")
        }

    def _format_streaming_positions(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Account": data.get("Account"),
            "Symbol": data.get("Symbol"),
            "Quantity": int(data.get("Quantity", 0)),
            "AveragePrice": float(data.get("AveragePrice", 0)),
            "MarketValue": float(data.get("MarketValue", 0)),
            "UnrealizedPL": float(data.get("UnrealizedPL", 0)),
            "RealizedPL": float(data.get("RealizedPL", 0)),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }

    def _format_streaming_orders(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Account": data.get("Account"),
            "Symbol": data.get("Symbol"),
            "OrderID": data.get("OrderID"),
            "OrderType": data.get("OrderType"),
            "Quantity": int(data.get("Quantity", 0)),
            "Price": float(data.get("Price", 0)),
            "Status": data.get("Status"),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }

    def _format_streaming_option_quotes(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Symbol": data.get("Symbol"),
            "Bid": float(data.get("Bid", 0)),
            "Ask": float(data.get("Ask", 0)),
            "Last": float(data.get("Last", 0)),
            "Volume": int(data.get("Volume", 0)),
            "OpenInterest": int(data.get("OpenInterest", 0)),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }

    def _format_streaming_quotes(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Symbol": data.get("Symbol"),
            "Bid": float(data.get("Bid", 0)),
            "Ask": float(data.get("Ask", 0)),
            "Last": float(data.get("Last", 0)),
            "Volume": int(data.get("Volume", 0)),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }

    def _format_streaming_option_chains(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Symbol": data.get("Symbol"),
            "Options": data.get("Options"),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }

    def _format_streaming_market_depth(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Symbol": data.get("Symbol"),
            "Bids": data.get("Bids"),
            "Asks": data.get("Asks"),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }
    def _format_streaming_aggregate_market_depth(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Symbol": data.get("Symbol"),
            "AggregatedBids": data.get("AggregatedBids"),
            "AggregatedAsks": data.get("AggregatedAsks"),
            "TimeStamp": str(self._ensure_timezone(data.get("TimeStamp"))) if "TimeStamp" in data else None
        }
    def _ensure_timezone(self, timestamp):
        if timestamp is None:
            return None
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp)
            except ValueError:
                logging.error(f"Invalid ISO format for timestamp: {timestamp}")
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        elif isinstance(timestamp, datetime):
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            return timestamp
        logging.error(f"Unsupported timestamp type: {type(timestamp)} - Value: {timestamp}")
        return None