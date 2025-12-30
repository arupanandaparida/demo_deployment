"""
Bybit WebSocket Collector - PURE WEBSOCKET DISCOVERY
Uses WebSocket snapshot feature to discover ALL active symbols dynamically
No REST API needed - works reliably on Railway
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

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.getenv('MYSQLHOST', 'localhost'),
    'port': int(os.getenv('MYSQLPORT', '3306')),
    'user': os.getenv('MYSQLUSER', 'root'),
    'password': os.getenv('MYSQLPASSWORD', ''),
    'database': os.getenv('MYSQLDATABASE', 'railway')
}

TABLE_NAME = 'bybit_data'

# Global state
db_queue = Queue(maxsize=10000)
update_count = 0
symbol_count = defaultdict(int)
price_cache = {}
symbol_data_cache = {}
discovered_symbols = {
    'linear': set(),
    'option': set()
}
connection_stats = {
    'linear_reconnects': 0,
    'option_reconnects': 0
}
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
    print(f"✅ Table '{TABLE_NAME}' ready\n")


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
        print(f"🧹 Old data ({last_date}) cleared. New day: {today}")
    else:
        print(f"✅ Same day ({today}). Data preserved.")

    cursor.close()
    conn.close()


def extract_expiry_from_symbol(symbol):
    """Extract expiry date from option symbol"""
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
    """Check if prices changed"""
    if mark == 0 and bid == 0 and ask == 0:
        return False
    
    key = f"{mark:.2f}|{bid:.2f}|{ask:.2f}"
    if price_cache.get(symbol) != key:
        price_cache[symbol] = key
        return True
    return False


def database_worker():
    """Background thread for database writes"""
    global update_count
    
    conn = connection_pool.get_connection()
    cursor = conn.cursor()
    
    batch = []
    batch_size = 10
    last_write = time.time()
    
    print("🔧 Database worker started\n")
    
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
                
                if update_count % 100 == 0:
                    print(f"💾 Updates: {update_count} | Queue: {db_queue.qsize()} | "
                          f"Symbols: {len(discovered_symbols['linear'])} linear + "
                          f"{len(discovered_symbols['option'])} options")
                
                batch = []
                last_write = time.time()
                
        except mysql.connector.Error as e:
            print(f"❌ DB Error: {e}")
            batch = []
            try:
                conn.close()
            except:
                pass
            conn = connection_pool.get_connection()
            cursor = conn.cursor()
        except Exception as e:
            print(f"❌ Worker Error: {e}")
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
    """Determine contract type"""
    if category == "option":
        if '-C-' in symbol or symbol.endswith('-C'):
            return 'call_option'
        elif '-P-' in symbol or symbol.endswith('-P'):
            return 'put_option'
        else:
            parts = symbol.split('-')
            if len(parts) >= 4:
                if parts[3] == 'C':
                    return 'call_option'
                elif parts[3] == 'P':
                    return 'put_option'
            return 'option'
    
    if 'PERP' in symbol.upper():
        return 'perpetual_future'
    
    if any(month in symbol for month in 
           ['JAN','FEB','MAR','APR','MAY','JUN',
            'JUL','AUG','SEP','OCT','NOV','DEC']):
        return 'dated_future'
    
    return 'perpetual_future'


def process_ticker_data(data, category):
    """Process ticker data and auto-discover symbols"""
    global symbol_count, symbol_data_cache, discovered_symbols
    
    try:
        symbol = data.get('symbol')
        if not symbol:
            return
        
        # Auto-discover and track this symbol
        if symbol not in discovered_symbols[category]:
            discovered_symbols[category].add(symbol)
            if len(discovered_symbols[category]) % 50 == 0:
                print(f"📊 Discovered {len(discovered_symbols[category])} {category} symbols")
        
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
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        record = (
            symbol, category, expiry, strike_price, contract_type,
            timestamp, mark_price,
            safe_float(cached.get('indexPrice')),
            last_price, best_bid, best_ask, bid_size, ask_size,
            safe_float(cached.get('volume24h')),
            safe_float(cached.get('turnover24h')),
            safe_float(cached.get('openInterest')),
            safe_float(cached.get('fundingRate')),
            safe_float(cached.get('predictedFundingRate')),
            safe_float(cached.get('delta')),
            safe_float(cached.get('gamma')),
            safe_float(cached.get('vega')),
            safe_float(cached.get('theta')),
            safe_float(cached.get('markIv')),
            safe_float(cached.get('underlyingPrice'))
        )
        
        try:
            db_queue.put_nowait(record)
        except:
            return
        
        symbol_count[f"{category}_{contract_type}"] += 1
        
    except Exception as e:
        pass


def on_message(ws, message, category):
    """Handle WebSocket messages - processes SNAPSHOT and DELTA"""
    try:
        data = json.loads(message)
        
        # Subscription confirmation
        if data.get('success') == True:
            return
        
        # Handle ticker data (both snapshot and delta)
        topic = data.get('topic', '')
        if topic.startswith('tickers'):
            msg_type = data.get('type')
            ticker_data = data.get('data')
            
            # Handle snapshot (initial data dump with ALL symbols)
            if msg_type == 'snapshot':
                if isinstance(ticker_data, list):
                    for ticker in ticker_data:
                        if isinstance(ticker, dict):
                            process_ticker_data(ticker, category)
                elif isinstance(ticker_data, dict):
                    process_ticker_data(ticker_data, category)
            
            # Handle delta (real-time updates)
            elif msg_type == 'delta':
                if isinstance(ticker_data, dict):
                    process_ticker_data(ticker_data, category)
                elif isinstance(ticker_data, list):
                    for ticker in ticker_data:
                        if isinstance(ticker, dict):
                            process_ticker_data(ticker, category)
                
    except:
        pass


def on_open_linear(ws):
    """Subscribe to ALL linear tickers - gets snapshot automatically"""
    global connection_stats
    connection_stats['linear_reconnects'] += 1
    
    print(f"✅ Linear WebSocket connected (#{connection_stats['linear_reconnects']})")
    
    # Subscribe to ALL tickers - Bybit will send snapshot of all symbols!
    payload = {
        "op": "subscribe",
        "args": ["tickers"]  # No specific symbol = ALL symbols!
    }
    
    try:
        ws.send(json.dumps(payload))
        print(f"📡 Subscribed to ALL linear tickers (snapshot mode)")
        print(f"⏳ Waiting for snapshot with all symbols...\n")
    except Exception as e:
        print(f"❌ Linear subscription error: {e}")


def on_open_option(ws):
    """Subscribe to ALL option tickers - gets snapshot automatically"""
    global connection_stats
    connection_stats['option_reconnects'] += 1
    
    print(f"✅ Option WebSocket connected (#{connection_stats['option_reconnects']})")
    
    # Subscribe to ALL tickers - Bybit will send snapshot of all symbols!
    payload = {
        "op": "subscribe",
        "args": ["tickers"]  # No specific symbol = ALL symbols!
    }
    
    try:
        ws.send(json.dumps(payload))
        print(f"📡 Subscribed to ALL option tickers (snapshot mode)")
        print(f"⏳ Waiting for snapshot with all symbols...\n")
    except Exception as e:
        print(f"❌ Option subscription error: {e}")


def start_linear_websocket():
    """Start linear WebSocket"""
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL_LINEAR,
                on_open=on_open_linear,
                on_message=lambda ws, msg: on_message(ws, msg, 'linear'),
                on_error=lambda ws, e: None,
                on_close=lambda ws, c, m: None
            )
            
            ws.run_forever(ping_interval=20, ping_timeout=10)
            
        except Exception as e:
            print(f"❌ Linear Error: {e}")
        
        print("🔄 Reconnecting linear...")
        time.sleep(5)


def start_option_websocket():
    """Start option WebSocket"""
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL_OPTION,
                on_open=on_open_option,
                on_message=lambda ws, msg: on_message(ws, msg, 'option'),
                on_error=lambda ws, e: None,
                on_close=lambda ws, c, m: None
            )
            
            ws.run_forever(ping_interval=20, ping_timeout=10)
            
        except Exception as e:
            print(f"❌ Option Error: {e}")
        
        print("🔄 Reconnecting options...")
        time.sleep(5)


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Bybit PURE WEBSOCKET Collector - Auto-Discovery Mode")
    print("=" * 70)
    print("📊 NO REST API - discovers ALL symbols via WebSocket snapshots")
    print("✨ Works reliably on Railway and any cloud platform")
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
            time.sleep(10)
    except KeyboardInterrupt:
        print(f"\n\n{'='*70}")
        print(f"👋 Stopped | Updates: {update_count}")
        print(f"📊 Discovered: {len(discovered_symbols['linear'])} linear, "
              f"{len(discovered_symbols['option'])} options")
        print(f"{'='*70}")
