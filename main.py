from flask import Flask, jsonify
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, timezone
from scipy.stats import norm
from scipy.optimize import brentq
import mysql.connector
from mysql.connector import pooling
from flask import Flask, jsonify, render_template, request
import requests
import threading
import time
from functools import wraps
import queue
import json


app = Flask(__name__)

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.getenv('MYSQLHOST', os.getenv('MYSQL_HOST', 'localhost')),
    'port': int(os.getenv('MYSQLPORT', os.getenv('MYSQL_PORT', '3306'))),
    'user': os.getenv('MYSQLUSER', os.getenv('MYSQL_USER', 'root')),
    'password': os.getenv('MYSQLPASSWORD', os.getenv('MYSQL_PASSWORD', '')),
    'database': os.getenv('MYSQLDATABASE', os.getenv('MYSQL_DATABASE', 'railway'))
}

# Exchange to Table mapping
EXCHANGE_TABLE_MAP = {
    'Delta': 'ticker_data',
    'Bybit': 'bybit_data'
}

# Database connection pool
class DatabasePool:
    def __init__(self, max_connections=10):
        self.pool = None
        self.max_connections = max_connections
        self.lock = threading.Lock()
        
        # Initialize MySQL connection pool
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="strategy_pool",
            pool_size=max_connections,
            **MYSQL_CONFIG
        )
    
    def get_connection(self, exchange='Delta'):
        """Get connection from pool"""
        try:
            return self.pool.get_connection()
        except mysql.connector.Error as e:
            print(f"Error getting connection: {e}")
            # Fallback: create new connection
            return mysql.connector.connect(**MYSQL_CONFIG)
    
    def return_connection(self, conn, exchange='Delta'):
        """Return connection to pool (handled automatically by connector)"""
        try:
            conn.close()
        except:
            pass

# Initialize database pool
db_pool = DatabasePool()

contract_cache = {
    'Delta': {'data': None, 'last_updated': 0},
    'Bybit': {'data': None, 'last_updated': 0},
    'cache_duration': 300
}

request_lock = threading.RLock()

def with_request_lock(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with request_lock:
            return func(*args, **kwargs)
    return wrapper


def parse_delta_symbol(symbol_str):
    """Parse Delta Exchange symbol format: C-BTC-90000-260625"""
    try:
        parts = symbol_str.split('-')
        if len(parts) != 4:
            return None
        
        option_type = 'PE' if parts[0] == 'P' else 'CE'
        underlying = parts[1]
        strike = int(parts[2])
        expiry_str = parts[3]
        expiry_date = datetime.strptime(expiry_str, "%d%m%y")
        
        return {
            'option_type': option_type,
            'underlying': underlying,
            'strike': strike,
            'expiry_date': expiry_date,
            'original_symbol': symbol_str
        }
    except Exception as e:
        print(f"Error parsing Delta symbol {symbol_str}: {str(e)}")
        return None


def parse_bybit_symbol(symbol_str):
    """Parse Bybit symbol format: BTC-26DEC25-90000-C or BTC-9JAN26-104000-P-USDT"""
    try:
        parts = symbol_str.split('-')
        
        # Bybit has two formats:
        # Standard: BTC-26DEC25-90000-C (4 parts)
        # USDT-settled: BTC-9JAN26-104000-P-USDT (5 parts)
        
        if len(parts) == 4:
            # Standard format
            underlying = parts[0]  # BTC, ETH
            expiry_str = parts[1]  # 26DEC25
            strike = int(parts[2])  # 90000
            option_type_letter = parts[3]  # C or P
        elif len(parts) == 5 and parts[4] == 'USDT':
            # USDT-settled format
            underlying = parts[0]  # BTC
            expiry_str = parts[1]  # 9JAN26
            strike = int(parts[2])  # 104000
            option_type_letter = parts[3]  # C or P
        else:
            return None
        
        # Convert C/P to CE/PE
        option_type = 'CE' if option_type_letter == 'C' else 'PE'
        
        # Parse expiry date: 26DEC25 or 9JAN26
        expiry_date = datetime.strptime(expiry_str, "%d%b%y")
        
        return {
            'option_type': option_type,
            'underlying': underlying,
            'strike': strike,
            'expiry_date': expiry_date,
            'original_symbol': symbol_str
        }
    except Exception as e:
        print(f"Error parsing Bybit symbol {symbol_str}: {str(e)}")
        return None


def load_ticker_data_from_db(exchange='Delta'):
    """Load ticker data from selected exchange table in MySQL"""
    table_name = EXCHANGE_TABLE_MAP.get(exchange, 'ticker_data')
    conn = db_pool.get_connection(exchange)
    
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        
        # Select parser based on exchange
        if exchange == 'Bybit':
            parse_symbol = parse_bybit_symbol
        else:
            parse_symbol = parse_delta_symbol
        
        parsed_data = []
        for _, row in df.iterrows():
            # Handle perpetual futures
            if row.get('contract_type') in ['perpetual_future', 'perpetual_futures', 'dated_future']:
                symbol_str = row['symbol']
                if 'BTC' in symbol_str:
                    underlying = 'BTC'
                elif 'ETH' in symbol_str:
                    underlying = 'ETH'
                else:
                    underlying = symbol_str.replace('USD', '').replace('USDT', '')
                
                parsed_data.append({
                    'symbol': row['symbol'],
                    'underlying': underlying,
                    'strike': None,
                    'option_type': None,
                    'expiry_date': None,
                    'expiry': row.get('expiry'),
                    'contract_type': 'perpetual_futures',
                    'mark_price': float(row.get('mark_price')) if row.get('mark_price') else None,
                    'best_bid': float(row.get('best_bid')) if row.get('best_bid') else None,
                    'best_ask': float(row.get('best_ask')) if row.get('best_ask') else None,
                    'spot_price': float(row.get('index_price')) if exchange == 'Bybit' and row.get('index_price') else (float(row.get('spot_price')) if row.get('spot_price') else None)
                })
            # Handle ALL option types including generic 'option'
            elif row.get('contract_type') in ['call_option', 'put_option', 'option', 'put_options', 'call_options']:
                parsed = parse_symbol(row['symbol'])
                if parsed:
                    parsed_data.append({
                        'symbol': row['symbol'],
                        'underlying': parsed['underlying'],
                        'strike': parsed['strike'],
                        'option_type': parsed['option_type'],
                        'expiry_date': parsed['expiry_date'],
                        'expiry': row.get('expiry'),
                        'contract_type': row.get('contract_type'),
                        'mark_price': float(row.get('mark_price')) if row.get('mark_price') else None,
                        'best_bid': float(row.get('best_bid')) if row.get('best_bid') else None,
                        'best_ask': float(row.get('best_ask')) if row.get('best_ask') else None,
                        'spot_price': float(row.get('index_price')) if exchange == 'Bybit' and row.get('index_price') else (float(row.get('spot_price')) if row.get('spot_price') else None)
                    })
        
        print(f"{exchange} - Loaded {len(parsed_data)} contracts from table {table_name}")
        result_df = pd.DataFrame(parsed_data)
        if not result_df.empty:
            print(f"{exchange} - Unique underlyings: {result_df['underlying'].unique()}")
            if result_df['expiry_date'].notna().any():
                unique_expiries = result_df['expiry_date'].dropna().unique()
                print(f"{exchange} - Unique expiry dates ({len(unique_expiries)}): {unique_expiries}")
        return result_df
    
    finally:
        db_pool.return_connection(conn, exchange)


def get_cached_contract_data(exchange='Delta'):
    """Get contract data with caching for selected exchange"""
    current_time = time.time()
    
    if (contract_cache[exchange]['data'] is None or 
        current_time - contract_cache[exchange]['last_updated'] > contract_cache['cache_duration']):
        
        print(f"Loading contract data from {exchange} table...")
        contract_val = load_ticker_data_from_db(exchange)
        contract_cache[exchange]['data'] = contract_val
        contract_cache[exchange]['last_updated'] = current_time
        print(f"{exchange} contract data cached successfully")
    
    return contract_cache[exchange]['data'].copy()


def get_market_data_batch(symbols, exchange='Delta', batch_size=500):
    """Get market data for symbols in batches"""
    if not symbols:
        return pd.DataFrame()

    table_name = EXCHANGE_TABLE_MAP.get(exchange, 'ticker_data')
    conn = db_pool.get_connection(exchange)
    
    try:
        all_data = []
        for i in range(0, len(symbols), batch_size):
            chunk = symbols[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(chunk))

            if exchange == 'Bybit':
                query = f"""
                    SELECT symbol,
                           mark_price as ltp,
                           best_bid as bidprice,
                           best_ask as askprice,
                           index_price as spot_price
                    FROM {table_name}
                    WHERE symbol IN ({placeholders})
                """
            else:
                query = f"""
                    SELECT symbol,
                           mark_price as ltp,
                           best_bid as bidprice,
                           best_ask as askprice,
                           spot_price
                    FROM {table_name}
                    WHERE symbol IN ({placeholders})
                """

            chunk_data = pd.read_sql(query, conn, params=chunk)
            
            # Convert Decimal to float
            for col in ['ltp', 'bidprice', 'askprice', 'spot_price']:
                if col in chunk_data.columns:
                    chunk_data[col] = chunk_data[col].astype(float)
            
            all_data.append(chunk_data)

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    finally:
        db_pool.return_connection(conn, exchange)


@app.route('/')
def strategy():
    return render_template('strategy.html')


@app.route('/api/options-metadata')
@with_request_lock
def get_options_metadata():
    try:
        exchange = request.args.get('exchange', 'Delta')
        selected_symbol = request.args.get('symbol', None)
        
        contract_val = get_cached_contract_data(exchange)
        
        # Filter for options only
        if exchange == 'Bybit':
            options_data = contract_val[
                contract_val['contract_type'].isin(['call_option', 'put_option'])
            ].copy()
        else:
            options_data = contract_val[
                contract_val['contract_type'].isin(['put_options', 'call_options'])
            ].copy()
        
        print(f"{exchange} - Total options before filtering: {len(options_data)}")
        
        def format_expiry(expiry_val):
            try:
                if pd.isna(expiry_val):
                    return None
                if isinstance(expiry_val, datetime):
                    return expiry_val.strftime('%d-%b-%Y')
                elif isinstance(expiry_val, str):
                    try:
                        dt = datetime.strptime(expiry_val, "%Y-%m-%d")
                        return dt.strftime('%d-%b-%Y')
                    except:
                        return expiry_val
                else:
                    return str(expiry_val)
            except Exception as e:
                print(f"Error formatting expiry {expiry_val}: {e}")
                return None
        
        # Use expiry_date (parsed from symbol), not expiry column
        options_data['expiry_formatted'] = options_data['expiry_date'].apply(format_expiry)
        
        # Drop rows with null expiry
        options_data = options_data[options_data['expiry_formatted'].notna()]
        
        # Debug: Print unique expiries
        print(f"{exchange} - All unique expiries: {options_data['expiry_formatted'].unique()}")
        
        # Filter by selected symbol if provided
        if selected_symbol:
            options_data = options_data[options_data['underlying'] == selected_symbol]
            print(f"{exchange} - After filtering for {selected_symbol}: {len(options_data)} contracts")
            print(f"{exchange} - Unique expiries for {selected_symbol}: {options_data['expiry_formatted'].unique()}")
        
        # Get unique combinations
        options = options_data[['underlying', 'expiry_formatted']].drop_duplicates()
        
        # Sort chronologically
        def parse_for_sort(expiry_str):
            try:
                return datetime.strptime(expiry_str, '%d-%b-%Y')
            except:
                return datetime.min
        
        options['sort_date'] = options['expiry_formatted'].apply(parse_for_sort)
        options = options.sort_values(['underlying', 'sort_date'])
        options = options.drop('sort_date', axis=1)
        
        options.columns = ['symboll', 'expiry_date']
        
        print(f"{exchange} - Returning {len(options)} unique symbol/expiry combinations")
        
        return jsonify({
            'options': options.to_dict(orient='records'),
            'futures': options.to_dict(orient='records')
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to fetch options: {str(e)}"}), 500


@app.route('/api/options-strategy')
@with_request_lock
def get_options():
    return get_options_metadata()


@app.route('/api/apply-option-strategy', methods=['POST'])
def apply_strategy():
    """Main strategy application endpoint"""
    start_time = time.time()
    
    try:
        # Get data first
        data = request.get_json()
        
        # Then extract exchange
        exchange = data.get('exchange', 'Delta')
        
        strategy = data.get('strategy')
        symbol = data.get('symbol')
        strike_interval = int(data.get('strike_interval'))
        gap = int(data.get('gap') or 0)
        no_portfolio = int(data.get('no_prt_folio'))
        option_expiry = data.get('option_expiry')
        future_expiry = data.get('future_expiry')
        
        strategy_lower = strategy.lower()
        
        print(f"\n=== Processing {strategy} for {symbol} on {exchange} ===")
        print(f"Option Expiry: {option_expiry}")
        print(f"Future Expiry: {future_expiry}")
        
        # Strategy validation
        if strategy_lower == 'jelly':
            if not option_expiry or not future_expiry:
                raise ValueError("Jelly strategy requires both option_expiry and future_expiry")
            if option_expiry == future_expiry:
                raise ValueError("Jelly strategy requires DIFFERENT expiries")
        
        elif strategy_lower == 'synthetic':
            if not option_expiry and not future_expiry:
                raise ValueError("Synthetic strategy requires expiry date")
            if not option_expiry:
                option_expiry = future_expiry
            if not future_expiry:
                future_expiry = option_expiry
            # Force same expiry for synthetic
            future_expiry = option_expiry
        
        elif strategy_lower in ('butterfly', 'ratio', 'pulse_butterfly'):
            if not option_expiry:
                option_expiry = future_expiry
            if not future_expiry:
                future_expiry = option_expiry
        
        result = process_strategy_with_optimization(
            strategy_lower, symbol, strike_interval, gap, no_portfolio,
            option_expiry, future_expiry, data, exchange
        )
        
        processing_time = time.time() - start_time
        print(f"Strategy {strategy} processed in {processing_time:.2f} seconds")
        
        return jsonify(result)
    
    except Exception as e:
        error_time = time.time() - start_time
        print(f"Error after {error_time:.2f} seconds: {str(e)}")
        return jsonify({"error": str(e)}), 400


def process_strategy_with_optimization(strategy_lower, symbol, strike_interval, gap, no_portfolio,
                                     option_expiry, future_expiry, data, exchange='Delta'):
    """Process strategy with optimization"""
    
    with request_lock:
        contract_val = get_cached_contract_data(exchange)
    
    # Filter by underlying
    contract_val = contract_val[contract_val['underlying'] == symbol]
    
    if contract_val.empty:
        raise ValueError(f"No contracts found for symbol {symbol}")
    
    # Debug: Print raw expiry input
    print(f"🔍 DEBUG - Raw option_expiry input: '{option_expiry}' (type: {type(option_expiry)})")
    print(f"🔍 DEBUG - Raw future_expiry input: '{future_expiry}' (type: {type(future_expiry)})")
    
    # Parse expiry
    try:
        expiry_dt = datetime.strptime(option_expiry, "%d-%b-%Y")
        print(f"✅ Parsed expiry_dt successfully: {expiry_dt} (date: {expiry_dt.date()})")
    except ValueError:
        try:
            expiry_dt = datetime.strptime(option_expiry, "%Y-%m-%d")
            print(f"✅ Parsed expiry_dt with alternate format: {expiry_dt} (date: {expiry_dt.date()})")
        except ValueError:
            print(f"❌ Failed to parse expiry_date: '{option_expiry}'")
            raise ValueError("Invalid expiry_date format")
    
    # Debug: Check available contracts
    print(f"\n🔍 DEBUG - Total contracts for {symbol}: {len(contract_val)}")
    print(f"🔍 DEBUG - Contract types: {contract_val['contract_type'].unique()}")
    
    # Filter option contracts based on exchange
    if exchange == 'Bybit':
        option_types = ['put_option', 'call_option', 'option']
    else:
        option_types = ['put_options', 'call_options', 'option']
    
    option_contracts = contract_val[
        contract_val['contract_type'].isin(option_types)
    ].copy()
    
    print(f"🔍 DEBUG - Option contracts after type filter: {len(option_contracts)}")
    
    if not option_contracts.empty:
        print(f"🔍 DEBUG - Sample expiry_date values from option_contracts:")
        print(option_contracts[['symbol', 'expiry_date', 'contract_type']].head(10))
        print(f"\n🔍 DEBUG - expiry_date dtype: {option_contracts['expiry_date'].dtype}")
        print(f"🔍 DEBUG - Unique expiry dates in option_contracts: {option_contracts['expiry_date'].unique()}")
    
    # Filter by expiry date
    option_contracts = option_contracts[
        (option_contracts['expiry_date'].dt.date == expiry_dt.date())
    ].copy()
    
    print(f"\n🔍 DEBUG - Option contracts after expiry filter: {len(option_contracts)}")
    
    if option_contracts.empty:
        print(f"❌ ERROR - No option contracts found!")
        print(f"Available expiry dates for {symbol}:")
        available_dates = contract_val[
            contract_val['contract_type'].isin(['put_options', 'call_options', 'put_option', 'call_option', 'option'])
        ]['expiry_date'].unique()
        for date in available_dates:
            print(f"  - {date}")
        raise ValueError(f"No option contracts found for {symbol} with expiry {option_expiry}")
    
    # Get perpetual futures data for Jelly/Synthetic
    futures_data = None
    if strategy_lower in ('synthetic', 'jelly'):
        futures_contracts = contract_val[
            contract_val['contract_type'] == 'perpetual_futures'
        ].copy()
        
        if futures_contracts.empty:
            raise ValueError(f"No perpetual futures found for {symbol}")
        
        futures_symbols = futures_contracts['symbol'].tolist()
        futures_market_data = get_market_data_batch(futures_symbols, exchange)
        futures_data = futures_contracts.merge(futures_market_data, on='symbol', how='left')
        print(f"Perpetual futures data: {len(futures_data)} rows")
    
    # Get spot price
    full_data = get_cached_contract_data(exchange)

    spot_price_data = full_data[
        (full_data['underlying'] == symbol) &
        (full_data['spot_price'].notna())
    ]['spot_price']
    
    if spot_price_data.empty:
        raise ValueError(f"No spot price found for {symbol}")
    
    spot_price = float(spot_price_data.iloc[0])
    print(f"Spot price for {symbol}: {spot_price}")
    
    # Get strike mode
    strike_mode = data.get("strike_mode", "auto")
    nearest_strike_input = data.get("nearest_strike")
    nearest_strike_ce_input = data.get("nearest_strike_ce")
    nearest_strike_pe_input = data.get("nearest_strike_pe")
    
    if strike_mode == "custom":
        try:
            nearest_strike = int(nearest_strike_input) if nearest_strike_input else None
            nearest_strike_ce = int(nearest_strike_ce_input) if nearest_strike_ce_input else None
            nearest_strike_pe = int(nearest_strike_pe_input) if nearest_strike_pe_input else None
        except ValueError:
            raise ValueError("Invalid strike value(s)")
    else:
        nearest_strike = round(spot_price / strike_interval) * strike_interval
        nearest_strike_ce = nearest_strike
        nearest_strike_pe = nearest_strike
    
    # Route to strategy (you need to implement these functions)
    if strategy_lower == "jelly":
        return process_jelly_strategy(
            option_contracts, futures_data, spot_price, nearest_strike, 
            strike_interval, no_portfolio, exchange
        )
    elif strategy_lower == "synthetic":
        return process_synthetic_strategy(
            option_contracts, futures_data, spot_price, nearest_strike, 
            strike_interval, no_portfolio, exchange
        )
    elif strategy_lower == "butterfly":
        return process_butterfly_strategy(
            option_contracts, spot_price, nearest_strike_ce, nearest_strike_pe,
            gap, no_portfolio, strike_interval, exchange
        )
    elif strategy_lower == "ratio":
        return process_ratio_strategy(
            option_contracts, spot_price, nearest_strike, gap, data, no_portfolio, strike_interval, exchange
        )
    elif strategy_lower == "pulse_butterfly":
        strategy_leg_type = data.get('strategy_leg_type')
        return process_Pulse_Butterfly_strategy(
            option_contracts, spot_price, nearest_strike_ce, nearest_strike_pe,
            gap, no_portfolio, strike_interval, strategy_leg_type, exchange
        )
    else:
        raise ValueError("Unsupported strategy")




def process_jelly_strategy(option_contracts, futures_data, spot_price, nearest_strike, 
                          strike_interval, no_portfolio, exchange='Delta'):
    return _process_jelly_or_synthetic(
        option_contracts, futures_data, spot_price, nearest_strike,
        strike_interval, no_portfolio, strategy='jelly', exchange=exchange
    )


def process_synthetic_strategy(option_contracts, futures_data, spot_price, nearest_strike, 
                               strike_interval, no_portfolio, exchange='Delta'):
    return _process_jelly_or_synthetic(
        option_contracts, futures_data, spot_price, nearest_strike,
        strike_interval, no_portfolio, strategy='synthetic', exchange=exchange
    )


def _process_jelly_or_synthetic(option_contracts, futures_data, spot_price,
                                nearest_strike, strike_interval, no_portfolio, strategy, exchange='Delta'):
    """Process jelly or synthetic using perpetual futures"""
    mround_strikes = [nearest_strike + i * strike_interval for i in range(-no_portfolio, no_portfolio + 1)]
    available_strikes = set(option_contracts['strike'].astype(int).unique())
    valid_strikes = [int(s) for s in mround_strikes if s in available_strikes]

    if not valid_strikes:
        raise ValueError("No valid strikes found")

    # Group options
    grouped = option_contracts[
        option_contracts['strike'].isin(valid_strikes)
    ].groupby(['underlying', 'expiry_date', 'strike', 'option_type'])['symbol'].first().reset_index()

    symbols = grouped['symbol'].tolist()
    ltp_data = get_market_data_batch(symbols, exchange)
    grouped = grouped.merge(ltp_data, on='symbol', how='left')

    # Get perpetual futures prices
    if futures_data is None or futures_data.empty:
        raise ValueError(f"Perpetual futures data required for {strategy} strategy")
    
    fut_row = futures_data.iloc[0]
    fut_ask = fut_row['askprice']
    fut_bid = fut_row['bidprice']
    fut_mark = fut_row.get('ltp', fut_row.get('mark_price', (fut_ask + fut_bid) / 2))
    
    if pd.isna(fut_ask) or pd.isna(fut_bid):
        raise ValueError("Perpetual futures bid/ask prices not available")
    
    print(f"\n=== {strategy.upper()} Strategy ===")
    print(f"Perpetual Futures - Bid: {fut_bid}, Ask: {fut_ask}, Mark: {fut_mark}")

    results = []
    for strike in valid_strikes:
        ce_row = grouped[(grouped['strike'] == strike) & (grouped['option_type'] == 'CE')]
        pe_row = grouped[(grouped['strike'] == strike) & (grouped['option_type'] == 'PE')]

        if ce_row.empty or pe_row.empty:
            continue

        ce_bid = ce_row['bidprice'].values[0]
        ce_ask = ce_row['askprice'].values[0]
        pe_bid = pe_row['bidprice'].values[0]
        pe_ask = pe_row['askprice'].values[0]
        ce_ltp = ce_row['ltp'].values[0]
        pe_ltp = pe_row['ltp'].values[0]

        if any(pd.isna(val) for val in [ce_bid, ce_ask, pe_bid, pe_ask, ce_ltp, pe_ltp]):
            continue

        # Conversion: Long Call + Short Put + Short Future
        # (CE_bid + Strike) - (PE_ask + Fut_ask)
        conversion = round((ce_bid + strike) - (pe_ask + fut_ask), 2)
        
        # Reversal: Short Call + Long Put + Long Future
        # (PE_bid + Fut_bid) - (CE_ask + Strike)
        reversal = round((pe_bid + fut_bid) - (ce_ask + strike), 2)

        # Debug print for first strike
        if strike == valid_strikes[0]:
            print(f"\nStrike {strike} Calculation:")
            print(f"CE: bid={ce_bid}, ask={ce_ask}")
            print(f"PE: bid={pe_bid}, ask={pe_ask}")
            print(f"Conversion = ({ce_bid} + {strike}) - ({pe_ask} + {fut_ask}) = {conversion}")
            print(f"Reversal = ({pe_bid} + {fut_bid}) - ({ce_ask} + {strike}) = {reversal}")

        results.append({
            "strategy": strategy,
            "strike": strike,
            "conversion": conversion,
            "reversal": reversal,
            "LTP": spot_price,
            "nearest_strike": nearest_strike,
            "ce_ltp": ce_ltp,
            "pe_ltp": pe_ltp,
            "fut_ask": fut_ask,
            "fut_bid": fut_bid
        })
    
    if not results:
        raise ValueError(f"No valid {strategy} portfolios found")
    
    print(f"Generated {len(results)} {strategy} portfolios")
    return results


def process_butterfly_strategy(option_contracts, spot_price, ce_strike, pe_strike, gap, no_portfolio, strike_interval, exchange='Delta'):
    """Process butterfly strategy"""
    def get_strike_range(center_strike):
        return np.arange(
            center_strike - no_portfolio * strike_interval,
            center_strike + (no_portfolio + 1) * strike_interval,
            strike_interval
        )

    ce_range = get_strike_range(ce_strike)
    pe_range = get_strike_range(pe_strike)
    strike_range = np.unique(np.concatenate([ce_range, pe_range]))

    available_strikes = option_contracts['strike'].astype(int).unique()
    valid_strikes = np.intersect1d(strike_range, available_strikes)

    if valid_strikes.size == 0:
        return {"error": "No valid strikes found"}, 400

    strike_l = valid_strikes
    strike_r = strike_l + gap
    strike_m = strike_l - gap
    combined_strikes = np.unique(np.concatenate([strike_l, strike_r, strike_m]))

    relevant_options = option_contracts[option_contracts['strike'].isin(combined_strikes)].copy()
    symbols = relevant_options['symbol'].tolist()
    ltp_data = get_market_data_batch(symbols, exchange)
    grouped = relevant_options.merge(ltp_data, on='symbol', how='left')

    def get_option_row(strike, opt_type):
        row = grouped[(grouped['strike'] == strike) & (grouped['option_type'] == opt_type)]
        return row.iloc[0] if not row.empty else None

    results = []

    for option_type in ['CE', 'PE']:
        center = ce_strike if option_type == 'CE' else pe_strike
        range_strikes = np.arange(
            center - no_portfolio * strike_interval,
            center + (no_portfolio + 1) * strike_interval,
            strike_interval
        )

        for l2 in range_strikes:
            l1 = l2 - gap
            l3 = l2 + gap

            if l1 not in valid_strikes or l3 not in valid_strikes:
                continue

            row_l1 = get_option_row(l1, option_type)
            row_l2 = get_option_row(l2, option_type)
            row_l3 = get_option_row(l3, option_type)

            if any(row is None for row in [row_l1, row_l2, row_l3]):
                continue

            try:
                prices = {
                    'l1': (row_l1['bidprice'], row_l1['askprice']),
                    'l2': (row_l2['bidprice'], row_l2['askprice']),
                    'l3': (row_l3['bidprice'], row_l3['askprice'])
                }

                if any(pd.isna(price) for bid, ask in prices.values() for price in [bid, ask]):
                    continue

                long_value = (prices['l2'][0] * 2) - (prices['l1'][1] + prices['l3'][1])
                short_value = (prices['l1'][0] + prices['l3'][0]) - (prices['l2'][1] * 2)

                l2_ltp = row_l2['ltp'] if 'ltp' in row_l2 else None
                if pd.isna(l2_ltp):
                    continue

                results.append({
                    "strategy": "butterfly",
                    "type": option_type,
                    "long_value": round(float(long_value), 2),
                    "short_value": round(float(short_value), 2),
                    "l2": int(l2),
                    "LTP": round(float(spot_price), 2),
                    "nearest_strike": int(ce_strike) if option_type == 'CE' else int(pe_strike),
                    "l2_ltp": round(float(l2_ltp), 2)
                })

            except Exception:
                continue

    if not results:
        raise ValueError("No valid butterfly portfolios found")

    return results


def process_ratio_strategy(option_contracts, spot_price, nearest_strike, gap, data, no_portfolio, strike_interval, exchange='Delta'):
    """Process ratio strategy"""
    ratio1 = int(data.get('ratio1', 0))
    ratio2 = int(data.get('ratio2', 0))
    if ratio1 == 0:
        raise ValueError("ratio1 cannot be zero")

    input1 = 1.0
    input2 = ratio2 / ratio1
    atm = nearest_strike

    strike_range = np.arange(atm - no_portfolio * strike_interval,
                             atm + (no_portfolio + 1) * strike_interval,
                             strike_interval)
    
    available_strikes = option_contracts['strike'].astype(int).unique()
    valid_strikes = np.intersect1d(strike_range, available_strikes)

    if valid_strikes.size == 0:
        return {"error": "No valid strikes found"}, 400

    strike_list_l = valid_strikes
    strike_list_r = strike_list_l + gap
    strike_mid = strike_list_l - gap
    combined_strike_list = np.unique(np.concatenate([strike_list_l, strike_list_r, strike_mid]))

    relevant_options = option_contracts[option_contracts['strike'].isin(combined_strike_list)].copy()
    symbols = relevant_options['symbol'].tolist()
    ltp_data = get_market_data_batch(symbols, exchange)
    merged = relevant_options.merge(ltp_data, on='symbol', how='left')
    merged = merged.dropna(subset=['bidprice', 'askprice', 'strike', 'option_type'])
    merged.set_index(['strike', 'option_type'], inplace=True)

    results = []
    option_types = ['CE', 'PE']
    merged_strikes = merged.index.get_level_values('strike').unique()

    for option_type in option_types:
        for l1 in valid_strikes:
            if option_type == 'CE':
                l2 = l1 + gap
            else:
                l2 = l1 - gap

            if l2 not in merged_strikes or (l1, option_type) not in merged.index or (l2, option_type) not in merged.index:
                continue

            row_l1 = merged.loc[(l1, option_type)]
            row_l2 = merged.loc[(l2, option_type)]

            buy_value = (row_l2['bidprice'] * input2) - (row_l1['askprice'] * input1)
            sell_value = (row_l1['bidprice'] * input1) - (row_l2['askprice'] * input2)

            l1_ltp = row_l1.get('ltp', None)
            if pd.isna(l1_ltp):
                continue

            results.append({
                "strategy": "ratio",
                "type": option_type,
                "l1": int(l1),
                "l2": int(l2),
                "buy_value": round(buy_value, 2),
                "sell_value": round(sell_value, 2),
                "l1_ltp": round(float(l1_ltp), 2)  
            })

    if not results:
        raise ValueError("No valid option data found")

    return {
        "strategy": "ratio",
        "input1": input1,
        "input2": input2,
        "gap": gap,
        "results": results,
        "LTP": spot_price,
        "nearest_strike": nearest_strike
    }



def process_Pulse_Butterfly_strategy(
    option_contracts, spot_price, nearest_strike_ce, nearest_strike_pe,
    gap, no_portfolio, strike_interval, strategy_leg_type
):
    """Process Pulse Butterfly strategy"""
    def get_strike_range(center_strike):
        if strategy_leg_type == '1331':
            max_leg_offset = 2 * gap
        else:
            max_leg_offset = 3 * gap
        extra_range = no_portfolio * gap + max_leg_offset

        start = (center_strike - extra_range) // strike_interval * strike_interval
        end = ((center_strike + extra_range + strike_interval - 1) // strike_interval) * strike_interval

        return list(range(start, end + 1, strike_interval))

    ce_range = get_strike_range(nearest_strike_ce)
    pe_range = get_strike_range(nearest_strike_pe)
    strike_range = sorted(set(ce_range + pe_range))

    relevant_options = option_contracts[option_contracts['strike'].isin(strike_range)].copy()
    symbols = relevant_options['symbol'].tolist()
    ltp_data = get_market_data_batch(symbols)
    grouped = relevant_options.merge(ltp_data, on='symbol', how='left')
    grouped['option_type'] = grouped['option_type'].str.strip().str.upper()

    grouped_unique = (
        grouped.sort_values(by=['bidprice', 'askprice'], ascending=[False, True])
        .groupby(['strike', 'option_type'], as_index=False)
        .first()
    )

    option_lookup = {
        (row['strike'], row['option_type']): row
        for _, row in grouped_unique.iterrows()
    }

    valid_strikes = sorted(grouped['strike'].unique())
    results = []

    for option_type in ['CE', 'PE']:
        center = nearest_strike_ce if option_type == 'CE' else nearest_strike_pe
        range_strikes = get_strike_range(center)

        for l2 in range_strikes:
            if option_type == 'CE':
                l1 = l2 - gap
                l3 = l2 + gap if strategy_leg_type == '1331' else l2 + 2 * gap
                l4 = l3 + gap
            else:
                l1 = l2 + gap
                l3 = l2 - gap if strategy_leg_type == '1331' else l2 - 2 * gap
                l4 = l3 - gap

            legs = [l1, l2, l3, l4]
            if any(s not in valid_strikes for s in legs):
                continue

            try:
                row_l1 = option_lookup.get((float(l1), option_type))
                row_l2 = option_lookup.get((float(l2), option_type))
                row_l3 = option_lookup.get((float(l3), option_type))
                row_l4 = option_lookup.get((float(l4), option_type))

                if any(row is None or not isinstance(row, pd.Series) or row.empty for row in [row_l1, row_l2, row_l3, row_l4]):
                    continue

                prices = {
                    'l1': (row_l1['bidprice'], row_l1['askprice']),
                    'l2': (row_l2['bidprice'], row_l2['askprice']),
                    'l3': (row_l3['bidprice'], row_l3['askprice']),
                    'l4': (row_l4['bidprice'], row_l4['askprice']),
                }

                if any(pd.isna(p) for bid, ask in prices.values() for p in [bid, ask]):
                    continue

                if strategy_leg_type == '1331':
                    long_value = (
                        (prices['l2'][0] * 3) + (prices['l4'][0] * 1)
                        - (prices['l1'][1] * 1 + prices['l3'][1] * 3)
                    )
                elif strategy_leg_type == '1221':
                    long_value = (
                        (prices['l2'][0] * 2) + (prices['l4'][0] * 1)
                        - (prices['l1'][1] * 1 + prices['l3'][1] * 2)
                    )
                    short_value = (
                        (prices['l1'][0] * 1 + prices['l3'][0] * 2)
                        - (prices['l2'][1] * 2 + prices['l4'][1] * 1)
                    )
                else:
                    continue

                if 'ltp' not in row_l2 or pd.isna(row_l2['ltp']):
                    continue
                l2_ltp = row_l2['ltp']

                results.append({
                    "strategy": "pulse_butterfly",
                    "type": option_type,
                    "long_value": round(long_value, 2),
                    "short_value": round(short_value, 2),
                    "l2": l2,
                    "LTP": round(float(spot_price), 2),
                    "nearest_strike": int(center),
                    "l2_ltp": round(float(l2_ltp), 2)
                })

            except Exception:
                continue

    if not results:
        raise ValueError("No valid pulse butterfly portfolios found")

    return results




# Connection pool
connection_pool = None

# Initialize MySQL database and connection pool
def init_db():
    global connection_pool
    
    try:
        # Create connection pool
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="config_pool",
            pool_size=5,
            **MYSQL_CONFIG
        )
        
        # Get connection to create table
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS configs (
                name VARCHAR(255) PRIMARY KEY,
                config TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Configuration table initialized successfully")
        
    except mysql.connector.Error as e:
        print(f"❌ Database initialization error: {e}")
        raise

# Get database connection from pool
def get_db():
    try:
        return connection_pool.get_connection()
    except mysql.connector.Error as e:
        print(f"Error getting connection: {e}")
        # Fallback: create new connection
        return mysql.connector.connect(**MYSQL_CONFIG)

@app.route('/api/configurations', methods=['GET'])
def get_configurations():
    """Get all configurations"""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute('SELECT name, config FROM configs')
        rows = cursor.fetchall()
        
        configs = {}
        for row in rows:
            try:
                configs[row['name']] = json.loads(row['config'])
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON for config '{row['name']}': {e}")
                continue
        
        cursor.close()
        conn.close()
        
        return jsonify(configs)
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        return jsonify({'error': 'Failed to retrieve configurations'}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/configurations', methods=['POST'])
def save_configuration():
    """Save or update a configuration"""
    try:
        data = request.json
        config_name = data.get('name')
        config_data = data.get('config')
        
        if not config_name or not config_data:
            return jsonify({'error': 'Missing name or config'}), 400
        
        # Validate config_name (e.g., no special characters)
        if not isinstance(config_name, str) or not config_name.strip():
            return jsonify({'error': 'Invalid configuration name'}), 400
        
        # Validate name length (MySQL VARCHAR(255) limit)
        if len(config_name) > 255:
            return jsonify({'error': 'Configuration name too long (max 255 characters)'}), 400

        # Try to serialize config_data to ensure it's valid
        try:
            config_json = json.dumps(config_data)
        except (TypeError, ValueError) as e:
            return jsonify({'error': 'Invalid configuration data format'}), 400

        conn = get_db()
        cursor = conn.cursor()
        
        # MySQL syntax: INSERT ... ON DUPLICATE KEY UPDATE
        cursor.execute('''
            INSERT INTO configs (name, config) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE 
                config = VALUES(config),
                updated_at = CURRENT_TIMESTAMP
        ''', (config_name, config_json))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'message': 'Configuration saved'}), 201
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    except json.JSONDecodeError as e:
        return jsonify({'error': 'Invalid JSON data'}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/configurations/<config_name>', methods=['GET'])
def get_configuration(config_name):
    """Get a specific configuration by name"""
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute('SELECT config FROM configs WHERE name = %s', (config_name,))
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not row:
            return jsonify({'error': 'Configuration not found'}), 404
        
        try:
            config = json.loads(row['config'])
        except json.JSONDecodeError as e:
            return jsonify({'error': 'Invalid configuration data'}), 500
        
        return jsonify(config)
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        return jsonify({'error': 'Failed to retrieve configuration'}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/configurations/<config_name>', methods=['DELETE'])
def delete_configuration(config_name):
    """Delete a configuration by name"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM configs WHERE name = %s', (config_name,))
        rows_affected = cursor.rowcount
        
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_affected == 0:
            return jsonify({'error': 'Configuration not found'}), 404
        
        return jsonify({'message': 'Configuration deleted'}), 200
        
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        return jsonify({'error': 'Failed to delete configuration'}), 500
    except Exception as e:
        print(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/configurations/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500
if __name__ == '__main__':
    init_db()
    # Get port from environment (Railway sets this)
    port = int(os.getenv('PORT', 5050))
    
    # Run app
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False  # Always False in production
    )
