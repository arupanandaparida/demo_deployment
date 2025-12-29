"""
Delta Exchange WebSocket Collector - Railway Production Version
"""
import websocket
import json
import mysql.connector
from mysql.connector import pooling
import time
import threading
from datetime import datetime
from queue import Queue
from collections import defaultdict
import os

# WebSocket URL
WS_URL = "wss://socket.india.delta.exchange"

# MySQL Configuration - Using environment variables
DB_CONFIG = {
     'host': os.getenv('MYSQLHOST', os.getenv('MYSQL_HOST', 'localhost')),
    'port': int(os.getenv('MYSQLPORT', os.getenv('MYSQL_PORT', '3306'))),
    'user': os.getenv('MYSQLUSER', os.getenv('MYSQL_USER', 'root')),
    'password': os.getenv('MYSQLPASSWORD', os.getenv('MYSQL_PASSWORD', '')),
    'database': os.getenv('MYSQLDATABASE', os.getenv('MYSQL_DATABASE', 'railway'))
}

# Globals
db_queue = Queue(maxsize=10000)
update_count = 0
symbol_count = defaultdict(int)
price_cache = {}
last_pong = time.time()
connection_pool = None


def setup_database():
    """Create MySQL table and connection pool"""
    global connection_pool
    
    print(f"📊 Connecting to MySQL: {DB_CONFIG['host']}")
    
    # Create connection pool
    try:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="delta_pool",
            pool_size=5,
            pool_reset_session=True,
            **DB_CONFIG
        )
        print(f"✅ Connection pool created")
    except mysql.connector.Error as e:
        print(f"❌ Failed to create connection pool: {e}")
        raise
    
    # Create table
    conn = connection_pool.get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_data (
            symbol VARCHAR(100) PRIMARY KEY,
            expiry DATE,
            strike_price DECIMAL(20,4),
            msg_type VARCHAR(50),
            contract_type VARCHAR(50),
            timestamp DATETIME(3),
            exchange_time DATETIME(3),
            mark_price DECIMAL(20,4),
            spot_price DECIMAL(20,4),
            best_bid DECIMAL(20,4),
            best_ask DECIMAL(20,4),
            bid_size DECIMAL(20,4),
            ask_size DECIMAL(20,4),
            delta DECIMAL(20,8),
            gamma DECIMAL(20,8),
            theta DECIMAL(20,8),
            vega DECIMAL(20,8),
            rho DECIMAL(20,8),
            mark_vol DECIMAL(20,8),
            open_interest DECIMAL(20,4),
            volume_24h DECIMAL(20,4),
            turnover_24h DECIMAL(20,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_timestamp (timestamp),
            INDEX idx_expiry (expiry),
            INDEX idx_contract_type (contract_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ''')

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Table 'ticker_data' ready\n")


def reset_table_if_new_day():
    """Clear table if last stored date is not today"""
    conn = connection_pool.get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DATE(MAX(created_at)) FROM ticker_data")
    result = cursor.fetchone()
    last_date = result[0] if result[0] else None

    today = datetime.now().strftime("%Y-%m-%d")

    if last_date is None:
        print("🆕 No previous data found. Starting fresh.")
    elif str(last_date) != today:
        cursor.execute("DELETE FROM ticker_data")
        conn.commit()
        print(f"🧹 Old data ({last_date}) cleared. New day: {today}")
    else:
        print(f"✅ Same day ({today}). Data preserved.")

    cursor.close()
    conn.close()


def extract_expiry_from_symbol(symbol):
    """Extract expiry date from symbol"""
    try:
        if '-' in symbol:
            parts = symbol.split('-')
            if len(parts) >= 4:
                date_str = parts[3]
                if len(date_str) == 6:
                    day = date_str[:2]
                    month = date_str[2:4]
                    year = date_str[4:]
                    return f"20{year}-{month}-{day}"
        
        if '_' in symbol:
            date_part = symbol.split('_')[1]
            if len(date_part) >= 5:
                dt = datetime.strptime(date_part, "%d%b%y")
                return dt.strftime("%Y-%m-%d")
    except:
        pass
    return None


def has_price_changed(symbol, mark, bid, ask):
    """Check if prices changed"""
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
                sql = '''
                    INSERT INTO ticker_data (
                        symbol, expiry, strike_price, msg_type, contract_type,
                        timestamp, exchange_time, mark_price, spot_price,
                        best_bid, best_ask, bid_size, ask_size,
                        delta, gamma, theta, vega, rho, mark_vol,
                        open_interest, volume_24h, turnover_24h
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        expiry=VALUES(expiry),
                        strike_price=VALUES(strike_price),
                        msg_type=VALUES(msg_type),
                        contract_type=VALUES(contract_type),
                        timestamp=VALUES(timestamp),
                        exchange_time=VALUES(exchange_time),
                        mark_price=VALUES(mark_price),
                        spot_price=VALUES(spot_price),
                        best_bid=VALUES(best_bid),
                        best_ask=VALUES(best_ask),
                        bid_size=VALUES(bid_size),
                        ask_size=VALUES(ask_size),
                        delta=VALUES(delta),
                        gamma=VALUES(gamma),
                        theta=VALUES(theta),
                        vega=VALUES(vega),
                        rho=VALUES(rho),
                        mark_vol=VALUES(mark_vol),
                        open_interest=VALUES(open_interest),
                        volume_24h=VALUES(volume_24h),
                        turnover_24h=VALUES(turnover_24h),
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


def determine_instrument_type(symbol, contract_type):
    """Determine instrument type"""
    if symbol.startswith('C-') or symbol.startswith('P-'):
        return 'OPTION'
    
    if 'PERP' in symbol.upper() or contract_type == 'perpetual_futures':
        return 'PERPETUAL'
    
    if '_' in symbol and any(m in symbol for m in ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']):
        return 'FUTURE'
    
    return 'PERPETUAL'


def process_ticker_data(data):
    """Process ticker data"""
    global symbol_count
    
    try:
        symbol = data.get('symbol')
        if not symbol:
            return
        
        if 'BTC' not in symbol and 'ETH' not in symbol:
            return
        
        quotes = data.get('quotes') or {}
        greeks = data.get('greeks') or {}
        
        mark_price = float(data.get('mark_price') or 0)
        best_bid = float(quotes.get('best_bid') or 0)
        best_ask = float(quotes.get('best_ask') or 0)
        
        if not has_price_changed(symbol, mark_price, best_bid, best_ask):
            return
        
        expiry = extract_expiry_from_symbol(symbol)
        strike_price = data.get('strike_price')
        contract_type = data.get('contract_type', '')
        msg_type = data.get('type', '')
        spot_price = data.get('spot_price')
        bid_size = quotes.get('bid_size')
        ask_size = quotes.get('ask_size')
        delta = greeks.get('delta')
        gamma = greeks.get('gamma')
        theta = greeks.get('theta')
        vega = greeks.get('vega')
        rho = greeks.get('rho')
        mark_vol = data.get('mark_vol')
        open_interest = data.get('oi')
        volume_24h = data.get('volume')
        turnover_24h = data.get('turnover')
        
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        exch_ts = data.get('timestamp')
        exchange_time = None
        if exch_ts:
            dt = datetime.fromtimestamp(exch_ts / 1_000_000)
            exchange_time = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        record = (
            symbol, expiry, strike_price, msg_type, contract_type,
            timestamp, exchange_time, mark_price, spot_price,
            best_bid, best_ask, bid_size, ask_size,
            delta, gamma, theta, vega, rho, mark_vol,
            open_interest, volume_24h, turnover_24h
        )
        
        try:
            db_queue.put_nowait(record)
        except:
            print(f"⚠️  Queue full, dropping: {symbol}")
            return
        
        coin = "BTC" if 'BTC' in symbol else "ETH"
        inst_type = determine_instrument_type(symbol, contract_type)
        count_key = f"{coin}_{inst_type}"
        symbol_count[count_key] += 1
        
        if symbol_count[count_key] % 10 == 0:
            if inst_type == 'OPTION':
                option_type = "CALL" if symbol.startswith('C-') else "PUT"
                print(f"⚡ {coin} {option_type} | {symbol} | Mark: ${mark_price:.1f}")
            else:
                print(f"⚡ {coin} {inst_type} | {symbol} | Mark: ${mark_price:.2f}")
            
    except Exception as e:
        print(f"❌ Processing Error: {e}")


def on_open(ws):
    """Connection established"""
    global last_pong
    last_pong = time.time()
    
    print("✅ Connected to Delta Exchange")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    subscribe(ws)


def subscribe(ws):
    """Subscribe to all symbols"""
    payload = {
        "type": "subscribe",
        "payload": {
            "channels": [{"name": "v2/ticker", "symbols": ["all"]}]
        }
    }
    
    try:
        ws.send(json.dumps(payload))
        print("📡 Subscribed to v2/ticker (ALL)")
        print("🔍 Filtering: BTC & ETH\n")
    except Exception as e:
        print(f"❌ Subscribe Error: {e}")


def on_message(ws, message):
    """Handle incoming messages"""
    try:
        data = json.loads(message)
        
        if data.get('type') == 'subscriptions':
            channels = data.get('channels', [])
            print(f"✅ Subscription confirmed: {len(channels)} channel(s)\n")
            return
        
        if data.get('type') == 'v2/ticker':
            process_ticker_data(data)
            
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"❌ Message Error: {e}")


def on_pong(ws, message):
    """Track pong"""
    global last_pong
    last_pong = time.time()


def on_error(ws, error):
    """Handle errors"""
    if error and "ping/pong" not in str(error).lower():
        print(f"❌ Socket Error: {error}")


def on_close(ws, code, msg):
    """Handle close"""
    print(f"\n🔌 Connection closed: {code} - {msg}")
    print("🔄 Reconnecting in 5 seconds...\n")
    time.sleep(5)


def start_websocket():
    """Start WebSocket connection"""
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_pong=on_pong,
        on_error=on_error,
        on_close=on_close
    )
    
    try:
        ws.run_forever(
            ping_interval=15,
            ping_timeout=10,
            skip_utf8_validation=True
        )
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        time.sleep(5)


if __name__ == "__main__":
    setup_database()
    reset_table_if_new_day()
    
    db_thread = threading.Thread(target=database_worker, daemon=True)
    db_thread.start()
    
    try:
        while True:
            start_websocket()
            print("🔄 Reconnecting...")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print(f"👋 Stopped")
        print(f"📊 Total Updates: {update_count}")
        for key, count in symbol_count.items():
            print(f"   {key}: {count}")
        print(f"   Queue: {db_queue.qsize()}")
        print("=" * 70)