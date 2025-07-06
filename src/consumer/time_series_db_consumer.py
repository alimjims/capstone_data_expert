import os
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer
import time
import logging

# Setup logging - only show important messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('kafka_consumer.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

def connect_to_database():
    """Connect to TimescaleDB"""
    logger.info("Connecting to TimescaleDB...")
    
    connection_string = os.getenv("TIMESCALE_CONNECTION_STRING")
    if not connection_string:
        logger.error("TIMESCALE_CONNECTION_STRING not found in environment")
        return None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.autocommit = True
        logger.info("Successfully connected to TimescaleDB")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to TimescaleDB: {e}")
        return None

def create_raw_stock_table(conn):
    """Create the stock_prices table"""
    logger.info("Setting up stock_prices table...")
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                timestamp TIMESTAMPTZ,
                symbol TEXT,
                price NUMERIC,
                volume BIGINT,
                open_price NUMERIC,
                high_price NUMERIC,
                low_price NUMERIC,
                close_price NUMERIC,
                vwap NUMERIC,
                accumulated_volume BIGINT,
                trade_count INTEGER,
                event_type TEXT
            );
        """)
        
        try:
            cursor.execute("SELECT create_hypertable('stock_prices', 'timestamp', if_not_exists => TRUE);")
            logger.info("Table setup complete")
        except Exception as e:
            logger.info("Table already exists")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return False

def create_kafka_consumer():
    """Create Kafka consumer"""
    logger.info("Setting up Kafka consumer...")
    
    # Check required environment variables
    required_vars = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_API_KEY', 'KAFKA_API_SECRET']
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing environment variable: {var}")
            return None
    
    config = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET"),
        'group.id': f'consumer_{int(time.time())}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 3000,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 500
    }
    
    try:
        consumer = Consumer(config)
        topic = 'polygon_stocks'
        consumer.subscribe([topic])
        logger.info(f"Kafka consumer ready - subscribed to {topic}")
        return consumer
        
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None

def wait_for_assignment(consumer, timeout=30):
    """Wait for consumer to get partition assignment"""
    logger.info("Waiting for Kafka partition assignment...")
    
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        msg = consumer.poll(1.0)
        
        try:
            assignment = consumer.assignment()
            if assignment:
                logger.info(f"Ready to consume from {len(assignment)} partitions")
                return msg
        except:
            pass
    
    logger.error("Timeout waiting for partition assignment")
    return None

def process_message(message_value):
    """Convert Kafka message to stock data"""
    try:
        raw_data = message_value.decode('utf-8')
        data = json.loads(raw_data)
        
        # Handle timestamp
        timestamp_data = data.get('timestamp')
        if timestamp_data and isinstance(timestamp_data, str):
            timestamp_str = timestamp_data.replace('Z', '+00:00') if 'Z' in timestamp_data else timestamp_data
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
            except ValueError:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # Helper function to safely convert to float
        def safe_float(value, default=0.0):
            if value is None:
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
        
        def safe_int(value, default=0):
            if value is None:
                return default
            try:
                return int(value)
            except (ValueError, TypeError):
                return default
        
        # Extract stock data with safe conversions
        stock_data = {
            'timestamp': timestamp,
            'symbol': data.get('symbol', 'UNKNOWN'),
            'price': safe_float(data.get('price') or data.get('close_price')),
            'volume': safe_int(data.get('volume')),
            'open_price': safe_float(data.get('open_price')),
            'high_price': safe_float(data.get('high_price')),
            'low_price': safe_float(data.get('low_price')),
            'close_price': safe_float(data.get('close_price')),
            'vwap': safe_float(data.get('vwap')),
            'accumulated_volume': safe_int(data.get('accumulated_volume')),
            'trade_count': safe_int(data.get('trade_count')),
            'event_type': data.get('event_type', 'aggregate')
        }
        
        return stock_data
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def insert_stock_data(conn, stock_data):
    """Insert stock record into database"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO stock_prices (
                timestamp, symbol, price, volume, 
                open_price, high_price, low_price, close_price,
                vwap, accumulated_volume, trade_count, event_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            stock_data['timestamp'],
            stock_data['symbol'],
            stock_data['price'],
            stock_data['volume'],
            stock_data['open_price'],
            stock_data['high_price'],
            stock_data['low_price'],
            stock_data['close_price'],
            stock_data['vwap'],
            stock_data['accumulated_volume'],
            stock_data['trade_count'],
            stock_data['event_type']
        ))
        
        return True
        
    except Exception as e:
        logger.error(f"Database insertion failed: {e}")
        return False

def main():
    """Main function"""
    logger.info("Starting Kafka Consumer")
    logger.info("=" * 40)
    
    # Setup
    db_conn = connect_to_database()
    if not db_conn:
        return 1
    
    if not create_raw_stock_table(db_conn):
        return 1
    
    kafka_consumer = create_kafka_consumer()
    if not kafka_consumer:
        return 1
    
    # Wait for assignment
    first_message = wait_for_assignment(kafka_consumer)
    if not kafka_consumer.assignment():
        logger.error("Failed to get partition assignment")
        return 1
    
    # Start processing
    logger.info("Starting message processing (periodic updates only)...")
    logger.info("Press Ctrl+C to stop")
    
    message_count = 0
    successful_inserts = 0
    error_count = 0
    start_time = time.time()
    last_status_time = time.time()
    
    # Process first message if we got one
    if first_message and not first_message.error():
        try:
            stock_data = process_message(first_message.value())
            if stock_data and insert_stock_data(db_conn, stock_data):
                successful_inserts += 1
            else:
                error_count += 1
            message_count += 1
        except Exception as e:
            logger.error(f"Error processing first message: {e}")
            logger.error(f"Raw first message: {first_message.value().decode('utf-8')[:200]}...")
            error_count += 1
    
    try:
        while True:
            msg = kafka_consumer.poll(1.0)
            
            if msg is None:
                # Show status every 60 seconds when no messages
                current_time = time.time()
                if current_time - last_status_time > 60:
                    logger.info(f"Waiting for messages... (processed: {message_count}, successful: {successful_inserts}, errors: {error_count})")
                    last_status_time = current_time
                continue
            
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            
            # Process message
            message_count += 1
            
            try:
                stock_data = process_message(msg.value())
                
                if stock_data is None:
                    error_count += 1
                    continue
                
                # Try to insert to database
                if insert_stock_data(db_conn, stock_data):
                    successful_inserts += 1
                else:
                    error_count += 1
                
                # Show progress every 100 messages
                if message_count % 100 == 0:
                    rate = message_count / (time.time() - start_time)
                    logger.info(f"Progress: {message_count} messages processed | {successful_inserts} successful DB inserts | {error_count} errors | {rate:.1f} msg/sec")
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing message #{message_count}: {e}")
                logger.error(f"Raw message: {msg.value().decode('utf-8')[:200]}...")
    
    except KeyboardInterrupt:
        logger.info("Stopping...")
    
    finally:
        runtime = time.time() - start_time
        logger.info("=" * 40)
        logger.info("FINAL STATISTICS")
        logger.info(f"Runtime: {runtime:.1f} seconds")
        logger.info(f"Total messages processed: {message_count}")
        logger.info(f"Successful database inserts: {successful_inserts}")
        logger.info(f"Errors: {error_count}")
        if message_count > 0:
            logger.info(f"Success rate: {(successful_inserts/message_count)*100:.1f}%")
            logger.info(f"Processing rate: {message_count/runtime:.1f} messages/second")
        
        kafka_consumer.close()
        db_conn.close()
        logger.info("Connections closed")
        
    return 0 if error_count == 0 else 1

if __name__ == "__main__":
    exit(main())