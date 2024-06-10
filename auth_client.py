#trading_module/auth_client.py

import httpx
import logging
from datetime import datetime, timedelta
import os

# Configure logging to send info messages to a file
logging.basicConfig(level=logging.INFO, filename='auth_client.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

class AuthClient:
    def __init__(self):
        self.client_id = os.getenv("TRADESTATION_CLIENT_ID")
        self.client_secret = os.getenv("TRADESTATION_CLIENT_SECRET")
        self.refresh_token = os.getenv("TRADESTATION_REFRESH_TOKEN")
        self.redirect_uri = os.getenv("TRADESTATION_REDIRECT_URI")
        self.base_url = os.getenv("TRADESTATION_BASE_URL")
        self.token_url = "https://signin.tradestation.com/oauth/token"
        self.access_token = None
        self.access_token_expiry = None
        self.client = httpx.AsyncClient()
        self._fetch_access_token_from_file()

    def _fetch_access_token_from_file(self, file_path='access_token.txt'):
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
                for line in lines:
                    if line.startswith("Access Token:"):
                        self.access_token = line.split("Access Token:")[1].strip()
                    elif line.startswith("Expiry Time:"):
                        self.access_token_expiry = datetime.strptime(line.split("Expiry Time:")[1].strip(), '%Y-%m-%d %H:%M:%S')
            logging.info(f"Fetched access token from file: {self.access_token}, expiry: {self.access_token_expiry}")
        except FileNotFoundError:
            logging.warning(f"Access token file not found: {file_path}")
        except Exception as e:
            logging.error(f"Failed to fetch access token from file: {e}")

    def _save_access_token_to_file(self, file_path='access_token.txt'):
        try:
            with open(file_path, 'w') as file:
                file.write(f"Access Token: {self.access_token}\n")
                file.write(f"Expiry Time: {self.access_token_expiry.strftime('%Y-%m-%d %H:%M:%S')}\n")
            logging.info("Saved access token to file.")
        except Exception as e:
            logging.error(f"Failed to save access token to file: {e}")

    async def _refresh_access_token(self):
        logging.info("Refreshing access token.")
        response = await self.client.post(
            self.token_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'refresh_token',
                'redirect_uri': self.redirect_uri,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token
            }
        )
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data['access_token']
            self.access_token_expiry = datetime.now() + timedelta(seconds=token_data['expires_in'] - 60)
            self._save_access_token_to_file()
            logging.info(f"Access token refreshed successfully: {self.access_token}, expiry: {self.access_token_expiry}")
        else:
            logging.error(f"Failed to refresh access token: {response.status_code} - {response.text}")

    async def _ensure_valid_access_token(self):
        if not self.access_token or self.access_token_expiry < datetime.now():
            await self._refresh_access_token()

    async def get_access_token(self):
        await self._ensure_valid_access_token()
        logging.info(f"Using access token: {self.access_token}")
        return self.access_token

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.aclose()