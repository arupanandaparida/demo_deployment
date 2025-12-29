"""
Flask API for Options Strategy Dashboard - Multi Exchange Support
Railway Production Version
"""
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# MySQL Configuration - Using environment variables for Railway
MYSQL_CONFIG = {
    'host': os.getenv('MYSQLHOST', os.getenv('MYSQL_HOST', 'localhost')),
    'port': int(os.getenv('MYSQLPORT', os.getenv('MYSQL_PORT', '3306'))),
    'user': os.getenv('MYSQLUSER', os.getenv('MYSQL_USER', 'root')),
    'password': os.getenv('MYSQLPASSWORD', os.getenv('MYSQL_PASSWORD', '')),
    'database': os.getenv('MYSQLDATABASE', os.getenv('MYSQL_DATABASE', 'railway'))
}

print(f"🔍 Connecting to MySQL: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}")
print(f"🔍 Database: {MYSQL_CONFIG['database']}")
print(f"🔍 User: {MYSQL_CONFIG['user']}")

# Exchange to Table mapping
EXCHANGE_TABLE_MAP = {
    'delta': 'ticker_data',
    'bybit': 'bybit_data'
}

# Connection pool
connection_pool = None


def init_connection_pool():
    """Initialize MySQL connection pool"""
    global connection_pool
    try:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="api_pool",
            pool_size=10,
            pool_reset_session=True,
            **MYSQL_CONFIG
        )
        print(f"✅ MySQL Pool initialized: {MYSQL_CONFIG['host']}")
    except mysql.connector.Error as e:
        print(f"❌ Failed to create connection pool: {e}")
        connection_pool = None


def get_db_connection():
    """Get database connection from pool"""
    global connection_pool
    
    if connection_pool is None:
        init_connection_pool()
    
    if connection_pool is None:
        return None
    
    try:
        conn = connection_pool.get_connection()
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Error getting connection: {e}")
        return None


def get_table_name(exchange='delta'):
    """Get table name for the specified exchange"""
    return EXCHANGE_TABLE_MAP.get(exchange.lower(), 'ticker_data')


def table_exists(table_name):
    """Check if table exists in database"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_name = %s
        """, (MYSQL_CONFIG['database'], table_name))
        
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        return False


def parse_delta_symbol(symbol_str):
    """Parse Delta Exchange symbol format"""
    try:
        parts = symbol_str.split('-')
        if len(parts) != 4:
            return None
        
        option_type = 'put' if parts[0] == 'P' else 'call'
        underlying = parts[1]
        strike = float(parts[2])
        
        expiry_str = parts[3]
        expiry_date = datetime.strptime(expiry_str, "%d%m%y").strftime("%Y-%m-%d")
        
        return {
            'option_type': option_type,
            'underlying': underlying,
            'strike': strike,
            'expiry': expiry_date
        }
    except Exception as e:
        print(f"❌ Parse error: {e}")
        return None


def parse_bybit_symbol(symbol_str):
    """Parse Bybit symbol format"""
    try:
        parts = symbol_str.split('-')
        
        if len(parts) < 4:
            return None
        
        underlying = parts[0]
        expiry_str = parts[1]
        strike = float(parts[2])
        option_letter = parts[3]
        
        option_type = 'call' if option_letter == 'C' else 'put'
        expiry_date = datetime.strptime(expiry_str, "%d%b%y").strftime("%Y-%m-%d")
        
        return {
            'option_type': option_type,
            'underlying': underlying,
            'strike': strike,
            'expiry': expiry_date
        }
    except Exception as e:
        print(f"❌ Parse error: {e}")
        return None


@app.route('/')
def index():
    return jsonify({
        "status": "running",
        "service": "Options Strategy API"
    })



@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    """Get available symbols"""
    try:
        exchange = request.args.get('exchange', 'delta').lower()
        table_name = get_table_name(exchange)
        
        if not table_exists(table_name):
            return jsonify({
                'error': f'Table {table_name} not found for {exchange}'
            }), 404
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        query = f'SELECT DISTINCT symbol FROM {table_name} WHERE symbol IS NOT NULL'
        cursor.execute(query)
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        symbols_set = set()
        
        for row in rows:
            symbol_str = row[0]
            
            if exchange == 'bybit':
                if symbol_str and len(symbol_str.split('-')) >= 1:
                    underlying = symbol_str.split('-')[0]
                    if underlying in ['BTC', 'ETH']:
                        symbols_set.add(underlying)
            else:
                parsed = parse_delta_symbol(symbol_str)
                if parsed:
                    symbols_set.add(parsed['underlying'])
        
        symbols = sorted(list(symbols_set))
        
        return jsonify({
            'symbols': symbols,
            'count': len(symbols),
            'exchange': exchange,
            'table': table_name
        })
        
    except Exception as e:
        print(f"❌ Error in get_symbols: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/expiry', methods=['GET'])
def get_expiry_dates():
    """Get available expiry dates"""
    try:
        exchange = request.args.get('exchange', 'delta').lower()
        symbol = request.args.get('symbol', '')
        table_name = get_table_name(exchange)
        
        if not symbol:
            return jsonify({'error': 'Symbol is required'}), 400
        
        if not table_exists(table_name):
            return jsonify({'error': f'Table {table_name} not found'}), 404
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        query = f'''
            SELECT DISTINCT symbol
            FROM {table_name}
            WHERE contract_type IN ('call_options', 'put_options', 'call_option', 'put_option', 'option')
            AND symbol IS NOT NULL
        '''
        cursor.execute(query)
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        expiry_dates_set = set()
        
        for row in rows:
            symbol_str = row[0]
            
            if exchange == 'bybit':
                parsed = parse_bybit_symbol(symbol_str)
                if parsed and parsed['underlying'] == symbol:
                    expiry_dates_set.add(parsed['expiry'])
            else:
                parsed = parse_delta_symbol(symbol_str)
                if parsed and parsed['underlying'] == symbol:
                    expiry_dates_set.add(parsed['expiry'])
        
        expiry_dates = sorted(list(expiry_dates_set))
        
        return jsonify({
            'expiry_dates': expiry_dates,
            'count': len(expiry_dates),
            'exchange': exchange,
            'symbol': symbol,
            'table': table_name
        })
        
    except Exception as e:
        print(f"❌ Error in get_expiry_dates: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/options-data', methods=['POST'])
def get_options_data():
    """Get options data for symbol and expiry"""
    try:
        data = request.get_json()
        exchange = data.get('exchange', 'delta').lower()
        symbol = data.get('symbol', '')
        expiry = data.get('expiry', '')
        table_name = get_table_name(exchange)
        
        if not symbol or not expiry:
            return jsonify({'error': 'Symbol and expiry required'}), 400
        
        if not table_exists(table_name):
            return jsonify({'error': f'Table {table_name} not found'}), 404
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # Get spot price
        if exchange == 'bybit':
            query = f'SELECT index_price FROM {table_name} WHERE symbol LIKE %s AND index_price IS NOT NULL LIMIT 1'
        else:
            query = f'SELECT spot_price FROM {table_name} WHERE symbol LIKE %s AND spot_price IS NOT NULL LIMIT 1'
        
        cursor.execute(query, (f'{symbol}%',) if exchange == 'bybit' else (f'%{symbol}%',))
        
        spot_result = cursor.fetchone()
        if not spot_result:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Spot price not found'}), 404
        
        spot_price = float(spot_result[0])
        
        # Get options data
        query = f'''
            SELECT symbol, best_bid, best_ask, mark_price,
                   delta, gamma, theta, vega, mark_vol, timestamp, contract_type
            FROM {table_name}
            WHERE contract_type IN ('call_options', 'put_options', 'call_option', 'put_option', 'option')
        '''
        cursor.execute(query)
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        strikes_data = {}
        
        for row in rows:
            symbol_str = row[0]
            
            if exchange == 'bybit':
                parsed = parse_bybit_symbol(symbol_str)
            else:
                parsed = parse_delta_symbol(symbol_str)
            
            if not parsed:
                continue
            
            if parsed['underlying'] != symbol or parsed['expiry'] != expiry:
                continue
            
            strike = parsed['strike']
            option_type = parsed['option_type']
            
            if strike not in strikes_data:
                strikes_data[strike] = {
                    'strike_price': strike,
                    'call': None,
                    'put': None
                }
            
            option_data = {
                'symbol': symbol_str,
                'best_bid': float(row[1]) if row[1] else None,
                'best_ask': float(row[2]) if row[2] else None,
                'mark_price': float(row[3]) if row[3] else None,
                'delta': float(row[4]) if row[4] else None,
                'gamma': float(row[5]) if row[5] else None,
                'theta': float(row[6]) if row[6] else None,
                'vega': float(row[7]) if row[7] else None,
                'mark_vol': float(row[8]) if row[8] else None,
                'timestamp': str(row[9]) if row[9] else None
            }
            
            if option_type == 'call':
                strikes_data[strike]['call'] = option_data
            else:
                strikes_data[strike]['put'] = option_data
        
        if not strikes_data:
            return jsonify({'error': 'No options data found'}), 404
        
        options_data = sorted(strikes_data.values(), key=lambda x: x['strike_price'])
        
        strikes = [x['strike_price'] for x in options_data]
        nearest_strike = min(strikes, key=lambda x: abs(x - spot_price))
        
        return jsonify({
            'success': True,
            'spot_price': spot_price,
            'nearest_strike': nearest_strike,
            'expiry': expiry,
            'symbol': symbol,
            'exchange': exchange,
            'table': table_name,
            'total_strikes': len(options_data),
            'data': options_data
        })
        
    except Exception as e:
        print(f"❌ Error in get_options_data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        stats = {}
        
        for exchange_name, table_name in EXCHANGE_TABLE_MAP.items():
            if not table_exists(table_name):
                stats[exchange_name] = {
                    'status': f'Table {table_name} not found',
                    'table': table_name
                }
                continue
            
            conn = get_db_connection()
            if not conn:
                stats[exchange_name] = {'status': 'Connection failed'}
                continue
            
            cursor = conn.cursor()
            
            cursor.execute(f'SELECT COUNT(DISTINCT symbol) FROM {table_name}')
            total_symbols = cursor.fetchone()[0]
            
            cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
            total_records = cursor.fetchone()[0]
            
            cursor.execute(f'SELECT MAX(timestamp) FROM {table_name}')
            last_update_result = cursor.fetchone()
            last_update = str(last_update_result[0]) if last_update_result[0] else None
            
            cursor.execute(f'''
                SELECT contract_type, COUNT(*) 
                FROM {table_name}
                GROUP BY contract_type
            ''')
            contract_types = {}
            for row in cursor.fetchall():
                contract_types[row[0]] = row[1]
            
            cursor.execute(f'''
                SELECT 
                    SUM(CASE WHEN symbol LIKE '%BTC%' THEN 1 ELSE 0 END) as btc_count,
                    SUM(CASE WHEN symbol LIKE '%ETH%' THEN 1 ELSE 0 END) as eth_count
                FROM {table_name}
            ''')
            coin_counts = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            stats[exchange_name] = {
                'status': 'Active',
                'table': table_name,
                'total_symbols': total_symbols,
                'total_records': total_records,
                'last_update': last_update,
                'contract_types': contract_types,
                'btc_symbols': coin_counts[0] if coin_counts else 0,
                'eth_symbols': coin_counts[1] if coin_counts else 0
            }
        
        return jsonify(stats)
        
    except Exception as e:
        print(f"❌ Error in get_stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'unhealthy', 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.fetchone()
        
        # Check tables
        tables_status = {}
        for exchange, table_name in EXCHANGE_TABLE_MAP.items():
            exists = table_exists(table_name)
            record_count = 0
            
            if exists:
                cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
                record_count = cursor.fetchone()[0]
            
            tables_status[exchange] = {
                'table': table_name,
                'exists': exists,
                'records': record_count
            }
        
        cursor.close()
        conn.close()
        
        # Check if any table has data
        has_data = any(t['records'] > 0 for t in tables_status.values() if t['exists'])
        
        return jsonify({
            'status': 'healthy' if has_data else 'no_data',
            'database': 'connected',
            'tables': tables_status,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'environment': os.getenv('RAILWAY_ENVIRONMENT', 'local')
        })
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


# Initialize connection pool on startup
init_connection_pool()


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("✅ Multi-Exchange Options Strategy API Server")
    print("=" * 70)
    print(f"🌍 Environment: {os.getenv('RAILWAY_ENVIRONMENT', 'local')}")
    print(f"📊 Database: {MYSQL_CONFIG['host']}")
    print("=" * 70)
    
    # Test database connection
    try:
        conn = get_db_connection()
        if conn:
            print(f"\n✓ MySQL Connected: {MYSQL_CONFIG['database']}")
            print("\nTable Status:")
            
            for exchange, table_name in EXCHANGE_TABLE_MAP.items():
                if table_exists(table_name):
                    cursor = conn.cursor()
                    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
                    count = cursor.fetchone()[0]
                    cursor.close()
                    print(f"  ✓ {exchange.upper():8} → {table_name:20} ({count:,} records)")
                else:
                    print(f"  ✗ {exchange.upper():8} → {table_name:20} (NOT FOUND)")
            
            conn.close()
        else:
            print("\n✗ MySQL Connection Failed")
    except Exception as e:
        print(f"\n✗ MySQL Error: {e}")
    
    print("\n" + "=" * 70)
    
    # Get port from environment (Railway sets this)
    port = int(os.getenv('PORT', 5050))
    
    # Run app
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False  # Always False in production
    )
