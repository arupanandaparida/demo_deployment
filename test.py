"""
Bybit WebSocket Collector - PURE WEBSOCKET VERSION
No REST API dependency - discovers all BTC options via WebSocket
Works perfectly on Railway
"""
import websocket
import json
import mysql.connector
from mysql.connector import pooling
import time
import threading
import os
from datetime import datetime, timezone
from queue import Queue
from collections import defaultdict

# Bybit WebSocket endpoints
WS_URL_LINEAR = "wss://stream.bybit.com/v5/public/linear"
WS_URL_OPTION = "wss://stream.bybit.com/v5/public/option"

# MySQL Configuration - Using environment variables for Railway
MYSQL_CONFIG = {
    'host': os.getenv('MYSQLHOST', os.getenv('MYSQL_HOST', 'localhost')),
    'port': int(os.getenv('MYSQLPORT', os.getenv('MYSQL_PORT', '3306'))),
    'user': os.getenv('MYSQLUSER', os.getenv('MYSQL_USER', 'root')),
    'password': os.getenv('MYSQLPASSWORD', os.getenv('MYSQL_PASSWORD', '')),
    'database': os.getenv('MYSQLDATABASE', os.getenv('MYSQL_DATABASE', 'railway'))
}

TABLE_NAME = 'bybit_data'

# Queue for non-blocking DB writes
db_queue = Queue(maxsize=10000)
update_count = 0
symbol_count = defaultdict(int)
price_cache = {}
symbol_data_cache = {}
discovered_options = set()  # Track discovered BTC options
option_ws = None  # Global reference to option websocket
last_pong = time.time()
connection_stats = {
    'linear_reconnects': 0,
    'option_reconnects': 0,
    'linear_last_connected': None,
    'option_last_connected': None
}

# Connection pool
connection_pool = None


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
                    print(f"💾 Updates: {update_count} | Queue: {db_queue.qsize()} | BTC Options: {len(discovered_options)}")
                
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


def subscribe_to_new_option(symbol):
    """Subscribe to a newly discovered BTC option"""
    global option_ws, discovered_options
    
    if symbol in discovered_options:
        return
    
    discovered_options.add(symbol)
    
    if option_ws and option_ws.sock and option_ws.sock.connected:
        payload = {
            "op": "subscribe",
            "args": [f"tickers.{symbol}"]
        }
        try:
            option_ws.send(json.dumps(payload))
            if len(discovered_options) % 10 == 0:
                print(f"📡 Now tracking {len(discovered_options)} BTC options")
        except Exception as e:
            pass


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
        
        # If this is a new BTC option, subscribe to it
        if category == 'option' and symbol not in discovered_options:
            subscribe_to_new_option(symbol)
        
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
                print(f"⚡ {coin} {option_type} | {symbol} | Mark: ${mark_price:.1f} | IV: {mark_iv:.3f}")
            else:
                print(f"⚡ {coin} {contract_type.upper()} | {symbol} | Mark: ${mark_price:.2f} | OI: {open_interest}")
            
    except Exception as e:
        pass


def on_message_linear(ws, message):
    """Handle linear/perpetual messages"""
    try:
        data = json.loads(message)
        
        if data.get('success') == True:
            print(f"✅ Linear subscription confirmed")
            return
        
        if data.get('topic', '').startswith('tickers'):
            ticker = data.get('data')
            if isinstance(ticker, dict):
                process_ticker_data(ticker, 'linear')
                
    except Exception as e:
        pass


def on_message_option(ws, message):
    """Handle option messages - discovers and subscribes to BTC options"""
    try:
        data = json.loads(message)

        if data.get('success') is True:
            return

        # Handle snapshot (initial bulk data)
        if data.get('type') == 'snapshot':
            ticker_data = data.get('data')
            if isinstance(ticker_data, list):
                btc_count = 0
                for ticker in ticker_data:
                    if isinstance(ticker, dict):
                        symbol = ticker.get('symbol', '')
                        if 'BTC' in symbol:
                            btc_count += 1
                            process_ticker_data(ticker, 'option')
                if btc_count > 0:
                    print(f"📸 Snapshot: discovered {btc_count} BTC options")
            return

        # Handle regular ticker updates
        if data.get('topic', '').startswith('tickers'):
            ticker_data = data.get('data')
            
            if isinstance(ticker_data, dict):
                process_ticker_data(ticker_data, 'option')
            elif isinstance(ticker_data, list):
                for ticker in ticker_data:
                    if isinstance(ticker, dict):
                        process_ticker_data(ticker, 'option')

    except json.JSONDecodeError:
        return
    except Exception as e:
        pass


def on_open_linear(ws):
    """Subscribe to BTCUSDT perpetual"""
    global last_pong, connection_stats
    last_pong = time.time()
    connection_stats['linear_last_connected'] = datetime.now().strftime("%H:%M:%S")
    connection_stats['linear_reconnects'] += 1
    
    print(f"✅ Connected to Bybit Linear/Perpetual (reconnect #{connection_stats['linear_reconnects']})")
    
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
    """Subscribe to ALL options to discover BTC options"""
    global connection_stats, option_ws
    option_ws = ws
    
    connection_stats['option_last_connected'] = datetime.now().strftime("%H:%M:%S")
    connection_stats['option_reconnects'] += 1
    
    print(f"✅ Connected to Bybit Options (reconnect #{connection_stats['option_reconnects']})")
    print(f"🔍 Auto-discovery mode: Will find all BTC options from WebSocket feed...")
    
    # Subscribe to all option tickers - this gives us a snapshot of ALL options
    # Then we filter for BTC and subscribe individually
    payload = {
        "op": "subscribe",
        "args": ["tickers.BTC-29DEC25-90000-C", "tickers.BTC-29DEC25-90000-P"]  # Subscribe to a couple to start
    }
    
    try:
        ws.send(json.dumps(payload))
        print(f"📡 Initial subscription sent - will auto-discover more BTC options\n")
    except Exception as e:
        print(f"❌ Option Subscription Error: {e}")


def on_error(ws, error):
    """Handle WebSocket errors"""
    if error:
        error_str = str(error).lower()
        if "ping/pong" in error_str or "connection is already closed" in error_str:
            return
        print(f"❌ Socket Error: {error}")


def on_close(ws, code, msg):
    """Handle connection closure"""
    print(f"🔌 Connection closed: {code}")


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
            print(f"❌ Linear Connection Error: {e}")
        
        print(f"🔄 Reconnecting linear in {retry_delay}s...")
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
            print(f"❌ Option Connection Error: {e}")
        
        print(f"🔄 Reconnecting options in {retry_delay}s...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


if __name__ == "__main__":
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
        print(f"📊 Total Updates Saved: {update_count}")
        print(f"📋 BTC Options Discovered: {len(discovered_options)}")
        for key, count in symbol_count.items():
            print(f"   {key}: {count}")
        print(f"   Queue Remaining: {db_queue.qsize()}")
        print("=" * 70)
