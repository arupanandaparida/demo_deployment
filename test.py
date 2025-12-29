"""
Bybit WebSocket Collector - HYBRID VERSION
Tries to fetch options from API, falls back to comprehensive hardcoded list
Works reliably on Railway with WebSocket-only mode
"""
import websocket
import json
import mysql.connector
from mysql.connector import pooling
import time
import threading
import os
import requests
from datetime import datetime, timezone, timedelta
from queue import Queue
from collections import defaultdict

# Bybit WebSocket endpoints
WS_URL_LINEAR = "wss://stream.bybit.com/v5/public/linear"
WS_URL_OPTION = "wss://stream.bybit.com/v5/public/option"

# Bybit REST API endpoint
BYBIT_API_URL = "https://api.bybit.com/v5/market/tickers"

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.getenv('MYSQLHOST', os.getenv('MYSQL_HOST', 'localhost')),
    'port': int(os.getenv('MYSQLPORT', os.getenv('MYSQL_PORT', '3306'))),
    'user': os.getenv('MYSQLUSER', os.getenv('MYSQL_USER', 'root')),
    'password': os.getenv('MYSQLPASSWORD', os.getenv('MYSQL_PASSWORD', '')),
    'database': os.getenv('MYSQLDATABASE', os.getenv('MYSQL_DATABASE', 'railway'))
}

TABLE_NAME = 'bybit_data'

# ✅ COMPREHENSIVE FALLBACK LIST - Generated for multiple expiries
def generate_comprehensive_option_list():
    """Generate comprehensive BTC option symbols for multiple expiries and strikes"""
    symbols = []
    
    # Common expiries (update these based on current available dates)
    expiries = [
        "30DEC25", "31DEC25",  # End of month
        "01JAN26", "09JAN26", "16JAN26", "02JAN26", "30JAN26",  # January
        "27FEB26", "27MAR26", "30JUN26", "29DEC26"  # Quarterly
    ]
    
    # Strike range: 60k to 120k in various increments
    strikes = []
    
    # Fine granularity around current price (80k-100k)
    for strike in range(80000, 100000, 500):
        strikes.append(strike)
    
    # Wider range with larger increments
    for strike in range(60000, 80000, 1000):
        strikes.append(strike)
    for strike in range(100000, 120000, 1000):
        strikes.append(strike)
    
    # Generate all combinations
    for expiry in expiries:
        for strike in strikes:
            symbols.append(f"BTC-{expiry}-{strike}-C-USDT")
            symbols.append(f"BTC-{expiry}-{strike}-P-USDT")
    
    return sorted(set(symbols))


# Generate comprehensive fallback list
FALLBACK_OPTION_SYMBOLS = generate_comprehensive_option_list()

# Queue for non-blocking DB writes
db_queue = Queue(maxsize=10000)
update_count = 0
symbol_count = defaultdict(int)
price_cache = {}
symbol_data_cache = {}
last_pong = time.time()
connection_stats = {
    'linear_reconnects': 0,
    'option_reconnects': 0,
    'linear_last_connected': None,
    'option_last_connected': None,
    'api_fetch_attempts': 0,
    'api_fetch_success': 0
}

connection_pool = None


def fetch_all_btc_options():
    """Try to fetch ALL active BTC options from Bybit REST API"""
    global connection_stats
    connection_stats['api_fetch_attempts'] += 1
    
    print("🔍 Attempting to fetch BTC options from Bybit API...")
    
    try:
        params = {
            'category': 'option',
            'baseCoin': 'BTC'
        }
        
        # Add headers to avoid rate limiting
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(
            BYBIT_API_URL, 
            params=params, 
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('retCode') == 0:
                symbols = []
                result_list = data.get('result', {}).get('list', [])
                
                for item in result_list:
                    symbol = item.get('symbol')
                    if symbol and 'BTC' in symbol:
                        symbols.append(symbol)
                
                if symbols:
                    connection_stats['api_fetch_success'] += 1
                    print(f"✅ API SUCCESS: Found {len(symbols)} active BTC options")
                    return symbols
        
        print(f"⚠️  API returned status {response.status_code}")
        return None
            
    except requests.exceptions.RequestException as e:
        print(f"⚠️  API request failed: {str(e)[:100]}")
        return None
    except Exception as e:
        print(f"⚠️  API error: {str(e)[:100]}")
        return None


def setup_database():
    """Create MySQL table and connection pool"""
    global connection_pool
    if connection_pool is not None:
        return
    
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="bybit_pool",
        pool_size=5,
        **MYSQL_CONFIG
    )
    
    conn = connection_pool.get_connection()
    cursor = conn.cursor()

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            symbol VARCHAR(100) PRIMARY KEY,
            category VARCHAR(50),
            expiry DATE,
            strike_price DECIMAL(20,4),
            contract_type VARCHAR(50),
            timestamp DATETIME(3),
            mark_price DECIMAL(20,4),
            index_price DECIMAL(20,4),
            last_price DECIMAL(20,4),
            best_bid DECIMAL(20,4),
            best_ask DECIMAL(20,4),
            bid_size DECIMAL(20,4),
            ask_size DECIMAL(20,4),
            volume_24h DECIMAL(20,4),
            turnover_24h DECIMAL(20,4),
            open_interest DECIMAL(20,4),
            funding_rate DECIMAL(20,8),
            predicted_funding_rate DECIMAL(20,8),
            delta DECIMAL(20,8),
            gamma DECIMAL(20,8),
            vega DECIMAL(20,8),
            theta DECIMAL(20,8),
            mark_iv DECIMAL(20,8),
            underlying_price DECIMAL(20,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_timestamp (timestamp),
            INDEX idx_expiry (expiry),
            INDEX idx_category (category),
            INDEX idx_contract_type (contract_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ''')

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Table '{TABLE_NAME}' ready in database '{MYSQL_CONFIG['database']}'\n")


def reset_table_if_new_day():
    """Clear table if last stored date is not today"""
    conn = connection_pool.get_connection()
    cursor = conn.cursor()

    cursor.execute(f"SELECT DATE(MAX(created_at)) FROM {TABLE_NAME}")
    result = cursor.fetchone()
    last_date = result[0] if result[0] else None
    
    today = datetime.now().strftime("%Y-%m-%d")

    if last_date is None:
        print("🆕 No previous data found. Starting fresh.")
    elif str(last_date) != today:
        cursor.execute(f"DELETE FROM {TABLE_NAME}")
        conn.commit()
        print(f"🧹 Old data ({last_date}) cleared. New trading day: {today}")
    else:
        print(f"✅ Same day ({today}). Data preserved.")

    cursor.close()
    conn.close()


def extract_expiry_from_symbol(symbol):
    """Extract expiry date from Bybit option symbol"""
    try:
        if '-' in symbol:
            parts = symbol.split('-')
            if len(parts) >= 3:
                date_str = parts[1]
                try:
                    dt = datetime.strptime(date_str, "%d%b%y")
                    return dt.strftime("%Y-%m-%d")
                except:
                    pass
    except:
        pass
    return None


def has_price_changed(symbol, mark, bid, ask):
    """Check if prices changed significantly"""
    if mark == 0 and bid == 0 and ask == 0:
        return False
    
    key = f"{mark:.2f}|{bid:.2f}|{ask:.2f}"
    if price_cache.get(symbol) != key:
        price_cache[symbol] = key
        return True
    return False


def database_worker():
    """Background thread for IMMEDIATE writes with small batches"""
    global update_count
    
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    
    batch = []
    batch_size = 10
    last_write = time.time()
    
    print("🔧 Database worker started (real-time mode)\n")
    
    while True:
        try:
            try:
                data = db_queue.get(timeout=0.1)
            except:
                data = None
            
            if data:
                batch.append(data)
            
            if batch and (len(batch) >= batch_size or time.time() - last_write > 0.1):
                sql = f'''
                    INSERT INTO {TABLE_NAME} (
                        symbol, category, expiry, strike_price, contract_type,
                        timestamp, mark_price, index_price, last_price,
                        best_bid, best_ask, bid_size, ask_size,
                        volume_24h, turnover_24h, open_interest,
                        funding_rate, predicted_funding_rate,
                        delta, gamma, vega, theta, mark_iv, underlying_price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        category=VALUES(category),
                        expiry=VALUES(expiry),
                        strike_price=VALUES(strike_price),
                        contract_type=VALUES(contract_type),
                        timestamp=VALUES(timestamp),
                        mark_price=VALUES(mark_price),
                        index_price=VALUES(index_price),
                        last_price=VALUES(last_price),
                        best_bid=VALUES(best_bid),
                        best_ask=VALUES(best_ask),
                        bid_size=VALUES(bid_size),
                        ask_size=VALUES(ask_size),
                        volume_24h=VALUES(volume_24h),
                        turnover_24h=VALUES(turnover_24h),
                        open_interest=VALUES(open_interest),
                        funding_rate=VALUES(funding_rate),
                        predicted_funding_rate=VALUES(predicted_funding_rate),
                        delta=VALUES(delta),
                        gamma=VALUES(gamma),
                        vega=VALUES(vega),
                        theta=VALUES(theta),
                        mark_iv=VALUES(mark_iv),
                        underlying_price=VALUES(underlying_price),
                        updated_at=CURRENT_TIMESTAMP
                '''
                
                cursor.executemany(sql, batch)
                conn.commit()
                update_count += len(batch)
                
                if update_count % 50 == 0:
                    print(f"💾 Updates: {update_count} | Queue: {db_queue.qsize()}")
                
                batch = []
                last_write = time.time()
                
        except mysql.connector.Error as e:
            print(f"❌ DB Worker Error: {e}")
            batch = []
            try:
                conn.close()
            except:
                pass
            conn = connection_pool.get_connection()
            cursor = conn.cursor()
        except Exception as e:
            print(f"❌ DB Worker Error: {e}")
            batch = []


def safe_float(value, default=0.0):
    """Safely convert to float"""
    try:
        if value is None or value == '':
            return default
        return float(value)
    except:
        return default


def determine_contract_type(symbol, category):
    """Determine specific contract type"""
    if category == "option":
        if '-C-' in symbol or symbol.endswith('-C-USDT'):
            return 'call_option'
        elif '-P-' in symbol or symbol.endswith('-P-USDT'):
            return 'put_option'
        else:
            parts = symbol.split('-')
            if len(parts) >= 4:
                option_letter = parts[3]
                if option_letter == 'C':
                    return 'call_option'
                elif option_letter == 'P':
                    return 'put_option'
            return 'option'
    
    if 'PERP' in symbol.upper() or 'USDT' in symbol:
        return 'perpetual_future'
    
    if any(month in symbol for month in 
           ['JAN','FEB','MAR','APR','MAY','JUN',
            'JUL','AUG','SEP','OCT','NOV','DEC']):
        return 'dated_future'
    
    return 'perpetual_future'


def process_ticker_data(data, category):
    """Process Bybit ticker data"""
    global symbol_count, symbol_data_cache
    
    try:
        symbol = data.get('symbol')
        if not symbol:
            return
        
        # Filter: Only BTC
        if 'BTC' not in symbol:
            return
        
        if symbol not in symbol_data_cache:
            symbol_data_cache[symbol] = {}
        
        symbol_data_cache[symbol].update({k: v for k, v in data.items() if v is not None and v != ''})
        
        cached = symbol_data_cache[symbol]
        
        mark_price = safe_float(cached.get('markPrice'))
        last_price = safe_float(cached.get('lastPrice'))
        
        if category == 'option':
            best_bid = safe_float(cached.get('bidPrice'))
            best_ask = safe_float(cached.get('askPrice'))
            bid_size = safe_float(cached.get('bidSize'))
            ask_size = safe_float(cached.get('askSize'))
        else:
            best_bid = safe_float(cached.get('bid1Price'))
            best_ask = safe_float(cached.get('ask1Price'))
            bid_size = safe_float(cached.get('bid1Size'))
            ask_size = safe_float(cached.get('ask1Size'))
        
        if mark_price == 0 and best_bid == 0 and best_ask == 0 and last_price == 0:
            return
        
        if not has_price_changed(symbol, mark_price, best_bid, best_ask):
            return
        
        expiry = extract_expiry_from_symbol(symbol)
        strike_price = None
        contract_type = determine_contract_type(symbol, category)
        
        if category == 'option' and '-' in symbol:
            parts = symbol.split('-')
            if len(parts) >= 4:
                strike_price = safe_float(parts[2])
        
        index_price = safe_float(cached.get('indexPrice'))
        volume_24h = safe_float(cached.get('volume24h'))
        turnover_24h = safe_float(cached.get('turnover24h'))
        open_interest = safe_float(cached.get('openInterest'))
        funding_rate = safe_float(cached.get('fundingRate'))
        predicted_funding_rate = safe_float(cached.get('predictedFundingRate'))
        delta = safe_float(cached.get('delta'))
        gamma = safe_float(cached.get('gamma'))
        vega = safe_float(cached.get('vega'))
        theta = safe_float(cached.get('theta'))
        mark_iv = safe_float(cached.get('markIv'))
        underlying_price = safe_float(cached.get('underlyingPrice'))
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        record = (
            symbol, category, expiry, strike_price, contract_type,
            timestamp, mark_price, index_price, last_price,
            best_bid, best_ask, bid_size, ask_size,
            volume_24h, turnover_24h, open_interest,
            funding_rate, predicted_funding_rate,
            delta, gamma, vega, theta, mark_iv, underlying_price
        )
        
        try:
            db_queue.put_nowait(record)
        except:
            return
        
        coin = "BTC"
        count_key = f"{coin}_{contract_type}"
        symbol_count[count_key] += 1
        
        if symbol_count[count_key] % 20 == 0:
            if contract_type in ['call_option', 'put_option']:
                option_type = "CALL" if contract_type == 'call_option' else "PUT"
                print(f"⚡ {coin} {option_type} | Strike: ${strike_price:.0f} | Mark: ${mark_price:.1f}")
            else:
                print(f"⚡ {coin} {contract_type.upper()} | Mark: ${mark_price:.2f} | OI: {open_interest}")
            
    except Exception as e:
        pass


def on_message_linear(ws, message):
    """Handle linear/perpetual messages"""
    try:
        data = json.loads(message)
        
        if data.get('success') == True:
            return
        
        if data.get('topic', '').startswith('tickers'):
            ticker = data.get('data')
            if isinstance(ticker, dict):
                process_ticker_data(ticker, 'linear')
                
    except Exception as e:
        pass


def on_message_option(ws, message):
    """Handle option messages"""
    try:
        data = json.loads(message)

        if data.get('success') is True:
            return

        if data.get('topic', '').startswith('tickers'):
            ticker_data = data.get('data')
            
            if isinstance(ticker_data, dict):
                process_ticker_data(ticker_data, 'option')
            elif isinstance(ticker_data, list):
                for ticker in ticker_data:
                    if isinstance(ticker, dict):
                        process_ticker_data(ticker, 'option')

    except:
        pass


def on_open_linear(ws):
    """Subscribe to BTCUSDT perpetual"""
    global last_pong, connection_stats
    last_pong = time.time()
    connection_stats['linear_last_connected'] = datetime.now().strftime("%H:%M:%S")
    connection_stats['linear_reconnects'] += 1
    
    print(f"✅ Linear WebSocket connected (#{connection_stats['linear_reconnects']})")
    
    payload = {
        "op": "subscribe",
        "args": ["tickers.BTCUSDT"]
    }
    
    try:
        ws.send(json.dumps(payload))
        print(f"📡 Subscribed to BTCUSDT perpetual\n")
    except Exception as e:
        print(f"❌ Linear Subscription Error: {e}")


def on_open_option(ws):
    """Subscribe to BTC options - try API first, fallback to comprehensive list"""
    global connection_stats
    connection_stats['option_last_connected'] = datetime.now().strftime("%H:%M:%S")
    connection_stats['option_reconnects'] += 1
    
    print(f"✅ Option WebSocket connected (#{connection_stats['option_reconnects']})")
    
    # Try to fetch from API
    option_symbols = fetch_all_btc_options()
    
    # Fallback to comprehensive generated list
    if not option_symbols:
        print(f"📋 Using fallback: {len(FALLBACK_OPTION_SYMBOLS)} generated option symbols")
        option_symbols = FALLBACK_OPTION_SYMBOLS
    else:
        print(f"✅ Using {len(option_symbols)} symbols from API")
    
    # Subscribe in batches
    batch_size = 100
    successful_batches = 0
    
    for i in range(0, len(option_symbols), batch_size):
        batch = option_symbols[i:i + batch_size]
        
        payload = {
            "op": "subscribe",
            "args": [f"tickers.{s}" for s in batch]
        }
        
        try:
            ws.send(json.dumps(payload))
            successful_batches += 1
            if i % 500 == 0:
                print(f"📡 Subscribed batch {successful_batches} ({i}/{len(option_symbols)})")
            time.sleep(0.2)
        except Exception as e:
            print(f"❌ Batch {i//batch_size + 1} failed: {e}")
    
    print(f"✅ Subscribed to {len(option_symbols)} BTC options ({successful_batches} batches)\n")


def on_error(ws, error):
    """Handle WebSocket errors"""
    if error:
        error_str = str(error).lower()
        if "ping/pong" in error_str or "connection is already closed" in error_str:
            return


def on_close(ws, code, msg):
    """Handle connection closure"""
    pass


def on_pong(ws, message):
    """Track pong responses"""
    global last_pong
    last_pong = time.time()


def start_linear_websocket():
    """Start WebSocket for linear/perpetual"""
    retry_delay = 2
    max_retry_delay = 30
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL_LINEAR,
                on_open=on_open_linear,
                on_message=on_message_linear,
                on_pong=on_pong,
                on_error=on_error,
                on_close=on_close
            )
            
            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                skip_utf8_validation=True
            )
            
            retry_delay = 2
            
        except Exception as e:
            print(f"❌ Linear Error: {e}")
        
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


def start_option_websocket():
    """Start WebSocket for options"""
    retry_delay = 2
    max_retry_delay = 30
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL_OPTION,
                on_open=on_open_option,
                on_message=on_message_option,
                on_pong=on_pong,
                on_error=on_error,
                on_close=on_close
            )
            
            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                skip_utf8_validation=True
            )
            
            retry_delay = 2
            
        except Exception as e:
            print(f"❌ Option Error: {e}")
        
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Bybit WebSocket Collector - HYBRID MODE")
    print("=" * 70)
    print(f"📊 Generated {len(FALLBACK_OPTION_SYMBOLS)} fallback option symbols")
    print("=" * 70 + "\n")
    
    setup_database()
    reset_table_if_new_day()
    
    db_thread = threading.Thread(target=database_worker, daemon=True)
    db_thread.start()
    
    linear_thread = threading.Thread(target=start_linear_websocket, daemon=True)
    option_thread = threading.Thread(target=start_option_websocket, daemon=True)
    
    linear_thread.start()
    option_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print(f"👋 Stopped")
        print(f"📊 Total Updates: {update_count}")
        print(f"🔄 API Attempts: {connection_stats['api_fetch_attempts']}")
        print(f"✅ API Success: {connection_stats['api_fetch_success']}")
        for key, count in symbol_count.items():
            print(f"   {key}: {count}")
        print("=" * 70)
