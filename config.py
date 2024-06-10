from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Environment Variables (recommended way to store secrets)
client_id = os.getenv("TRADESTATION_CLIENT_ID")
client_secret = os.getenv("TRADESTATION_CLIENT_SECRET")
refresh_token = os.getenv("TRADESTATION_REFRESH_TOKEN")
redirect_uri = os.getenv("TRADESTATION_REDIRECT_URI")
base_url = os.getenv("TRADESTATION_BASE_URL")
polygon_key = os.getenv("POLYGON_KEY")
polygon_base_url = os.getenv("POLYGON_BASE_URL")
