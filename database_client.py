# trading_module/database_client.py

import psycopg2
from psycopg2 import pool
from typing import List, Dict, Any
import logging
import datetime

class DatabaseClient:
    def __init__(self, dbname="blotter", user="Brent", password="Tangosg1", host="127.0.0.1", port="5432"):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, user=user, password=password, host=host, port=port, database=dbname
        )

    def _execute_query(self, query: str, params: Dict[str, Any] = None):
        conn = self.connection_pool.getconn()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Error executing query: {e}")
        finally:
            cursor.close()
            self.connection_pool.putconn(conn)

    def _ensure_timezone(self, timestamp_str):
        if timestamp_str:
            dt = datetime.datetime.fromisoformat(timestamp_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt
        return None

    def create_table(self, table_name: str, schema: str):
        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({schema})'
        self._execute_query(create_table_query)

    def insert_data(self, table_name: str, data: Dict[str, Any]):
        columns = ', '.join(data.keys())
        values = ', '.join([f'%({key})s' for key in data.keys()])
        insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'
        self._execute_query(insert_query, data)

    def create_quotes_table(self):
        schema = '''
            Symbol TEXT, Open REAL, High REAL, Low REAL, PreviousClose REAL, Last REAL, Ask REAL, AskSize INTEGER,
            Bid REAL, BidSize INTEGER, NetChange REAL, NetChangePct REAL, High52Week REAL, High52WeekTimestamp TIMESTAMP WITH TIME ZONE,
            Low52Week REAL, Low52WeekTimestamp TIMESTAMP WITH TIME ZONE, Volume INTEGER, PreviousVolume INTEGER, Close REAL,
            DailyOpenInterest INTEGER, TradeTime TIMESTAMP WITH TIME ZONE, TickSizeTier TEXT, MarketFlags JSONB, LastSize INTEGER,
            LastVenue TEXT, VWAP REAL
        '''
        self.create_table('quotes', schema)

    def create_option_quotes_table(self):
        schema = '''
            Symbol TEXT, Last REAL, Delta REAL, Theta REAL, Gamma REAL, Rho REAL, Vega REAL, ImpliedVolatility REAL,
            IntrinsicValue REAL, ExtrinsicValue REAL, TheoreticalValue REAL, ProbabilityITM REAL, ProbabilityOTM REAL,
            ProbabilityBE REAL, ProbabilityITM_IV REAL, ProbabilityOTM_IV REAL, ProbabilityBE_IV REAL, TheoreticalValue_IV REAL,
            DailyOpenInterest INTEGER, Ask REAL, Bid REAL, Mid REAL, AskSize INTEGER, BidSize INTEGER, Close REAL, High REAL,
            Last REAL, Low REAL, NetChange REAL, NetChangePct REAL, Open REAL, PreviousClose REAL, Volume INTEGER, Underlying TEXT,
            Expiration TEXT, OptionType TEXT, AssetType TEXT, Ratio INTEGER, StrikePrice REAL
        '''
        self.create_table('option_quotes', schema)

    def create_options_chain_table(self):
        schema = '''
            Delta REAL, Theta REAL, Gamma REAL, Rho REAL, Vega REAL, ImpliedVolatility REAL, IntrinsicValue REAL, ExtrinsicValue REAL,
            TheoreticalValue REAL, ProbabilityITM REAL, ProbabilityOTM REAL, ProbabilityBE REAL, ProbabilityITM_IV REAL,
            ProbabilityOTM_IV REAL, ProbabilityBE_IV REAL, TheoreticalValue_IV REAL, DailyOpenInterest INTEGER, Ask REAL, Bid REAL,
            Mid REAL, AskSize INTEGER, BidSize INTEGER, Close REAL, High REAL, Last REAL, Low REAL, NetChange REAL, NetChangePct REAL,
            Open REAL, PreviousClose REAL, Volume INTEGER, Symbol TEXT, Expiration TEXT, OptionType TEXT, AssetType TEXT,
            Ratio INTEGER, StrikePrice REAL
        '''
        self.create_table('options_chain', schema)

    def create_bars_table(self):
        schema = '''
            High REAL, Low REAL, Open REAL, Close REAL, TimeStamp TIMESTAMP WITH TIME ZONE, TotalVolume INTEGER, DownTicks INTEGER,
            DownVolume INTEGER, OpenInterest INTEGER, IsRealtime BOOLEAN, IsEndOfHistory BOOLEAN, TotalTicks INTEGER, UnchangedTicks INTEGER,
            UnchangedVolume INTEGER, UpTicks INTEGER, UpVolume INTEGER, Epoch BIGINT, BarStatus TEXT
        '''
        self.create_table('bars', schema)

    def create_positions_table(self):
        schema = '''
            AccountID TEXT, AveragePrice REAL, AssetType TEXT, Last REAL, Bid REAL, Ask REAL, ConversionRate REAL, DayTradeRequirement REAL,
            InitialRequirement REAL, PositionID TEXT, LongShort TEXT, Quantity REAL, Symbol TEXT, Timestamp TIMESTAMP WITH TIME ZONE,
            TodaysProfitLoss REAL, TotalCost REAL, MarketValue REAL, MarkToMarketPrice REAL, UnrealizedProfitLoss REAL,
            UnrealizedProfitLossPercent REAL, UnrealizedProfitLossQty REAL
        '''
        self.create_table('positions', schema)

    def create_orders_table(self):
        schema = '''
            OrderID TEXT, AccountID TEXT, Currency TEXT, OrderType TEXT, Status TEXT, StatusDescription TEXT, Duration TEXT,
            GoodTillDate TIMESTAMP WITH TIME ZONE, OpenedDateTime TIMESTAMP WITH TIME ZONE, Routing TEXT, PriceUsedForBuyingPower REAL,
            CommissionFee REAL, UnbundledRouteFee REAL, Legs JSONB, MarketActivationRules JSONB, TimeActivationRules JSONB,
            ConditionalOrders JSONB, AdvancedOptions TEXT, TrailingStop JSONB, StopPrice REAL
        '''
        self.create_table('orders', schema)

    def insert_quote(self, quote: Dict[str, Any]):
        if 'High52WeekTimestamp' in quote:
            quote['High52WeekTimestamp'] = self._ensure_timezone(quote.get('High52WeekTimestamp'))
        if 'Low52WeekTimestamp' in quote:
            quote['Low52WeekTimestamp'] = self._ensure_timezone(quote.get('Low52WeekTimestamp'))
        if 'TradeTime' in quote:
            quote['TradeTime'] = self._ensure_timezone(quote.get('TradeTime'))
        self.insert_data('quotes', quote)

    def insert_option_quote(self, option_quote: Dict[str, Any]):
        self.insert_data('option_quotes', option_quote)

    def insert_option_chain(self, option_chain: Dict[str, Any]):
        self.insert_data('options_chain', option_chain)

    def insert_bar(self, bar: Dict[str, Any]):
        if 'TimeStamp' in bar:
            bar['TimeStamp'] = self._ensure_timezone(bar.get('TimeStamp'))
        self.insert_data('bars', bar)

    def insert_position(self, position: Dict[str, Any]):
        if 'Timestamp' in position:
            position['Timestamp'] = self._ensure_timezone(position.get('Timestamp'))
        self.insert_data('positions', position)

    def insert_order(self, order: Dict[str, Any]):
        if 'GoodTillDate' in order:
            order['GoodTillDate'] = self._ensure_timezone(order.get('GoodTillDate'))
        if 'OpenedDateTime' in order:
            order['OpenedDateTime'] = self._ensure_timezone(order.get('OpenedDateTime'))
        self.insert_data('orders', order)