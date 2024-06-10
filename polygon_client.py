#trading_module/polygon_client.py

import httpx
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
from trading_module import config

class PolygonClient:
    def __init__(self):
        self.api_key = config.polygon_api_key
        self.base_url = config.polygon_base_url

    def get_agg_data(self, ticker, multiplier='1', timespan='minute', start_date=None, end_date=None):
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

            data_list = []
            url = f"{self.base_url}/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?apiKey={self.api_key}"
            
            while url:
                with httpx.Client() as client:
                    response = client.get(url)
                    response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
                    data = response.json()  # Parse the JSON response
                    if 'results' in data:
                        data_list.extend(data['results'])
                    
                    # Get the next URL for pagination
                    if 'next_url' in data:
                        next_url = data['next_url']
                        parsed_url = urlparse(next_url)
                        query_params = parse_qs(parsed_url.query)
                        cursor = query_params.get('cursor', [None])[0]
                        
                        # Append API key to cursor if available
                        if cursor:
                            next_url += f"&apiKey={self.api_key}"
                        url = next_url
                    else:
                        url = None

            return data_list
        except httpx.HTTPError as exc:
            print(f"HTTP error occurred: {exc}")
            return None
        except Exception as exc:
            print(f"An unexpected error occurred: {exc}")
            return None