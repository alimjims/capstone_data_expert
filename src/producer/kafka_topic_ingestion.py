import json
import os
import time
import threading
import logging
from datetime import datetime
from dotenv import load_dotenv
from polygon import WebSocketClient
from confluent_kafka import Producer, KafkaException

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('polygon_kafka.log'), logging.StreamHandler()]
)

# Load environment variables
load_dotenv()

# Configuration
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
KAFKA_TOPIC = "polygon_stocks"

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "RBLX", "COIN"]

# Counters and stats
message_count = 0
processed_count = 0
kafka_success_count = 0
kafka_error_count = 0

# Throttling mechanism - store last update time for each symbol
last_update_time = {}
UPDATE_INTERVAL = 1  # seconds between updates per symbol

# Kafka producer (will be initialized in main)
producer = None

def should_process_symbol(symbol):
    """
    Check if enough time has passed since last update for this symbol
    """
    current_time = time.time()
    
    if symbol not in last_update_time:
        last_update_time[symbol] = current_time
        return True
    
    time_since_last = current_time - last_update_time[symbol]
    
    if time_since_last >= UPDATE_INTERVAL:
        last_update_time[symbol] = current_time
        return True
    
    return False

def delivery_report(err, msg):
    """Callback for Kafka message delivery"""
    global kafka_success_count, kafka_error_count
    
    if err is not None:
        kafka_error_count += 1
        logging.error(f'Kafka delivery failed: {err}')
    else:
        kafka_success_count += 1
        if kafka_success_count % 50 == 0:  # Log every 50 successful deliveries
            logging.info(f'Successfully delivered {kafka_success_count} messages to Kafka')

def process_polygon_message(msg):
    """
    Process individual Polygon message and return formatted data
    """
    # Handle EquityAgg objects directly (most common case)
    if hasattr(msg, 'event_type') and msg.event_type == 'A':
        return {
            "event_type": "aggregate",
            "symbol": msg.symbol,
            "timestamp": datetime.fromtimestamp(msg.start_timestamp / 1000).isoformat(),
            "price": msg.close,  # Current price
            "open": msg.open,
            "high": msg.high,
            "low": msg.low,
            "close": msg.close,
            "volume": msg.volume,
            "vwap": msg.vwap,
            "accumulated_volume": getattr(msg, 'accumulated_volume', None),
            "trade_count": getattr(msg, 'average_size', None)
        }
    
    # Handle other message types with 'ev' attribute (legacy format)
    elif hasattr(msg, 'ev'):  # Event type field
        event_type = msg.ev
        
        if event_type == 'A':  # Aggregate/OHLCV data
            return {
                "event_type": "aggregate",
                "symbol": getattr(msg, 'sym', getattr(msg, 'symbol', None)),
                "timestamp": getattr(msg, 's', None),  # start timestamp
                "open": getattr(msg, 'o', None),
                "high": getattr(msg, 'h', None),
                "low": getattr(msg, 'l', None),
                "close": getattr(msg, 'c', None),
                "volume": getattr(msg, 'v', None),
                "trade_count": getattr(msg, 'n', None),
                "vwap": getattr(msg, 'vw', None)
            }
            
        elif event_type == 'T':  # Trade data
            return {
                "event_type": "trade",
                "symbol": getattr(msg, 'sym', getattr(msg, 'symbol', None)),
                "timestamp": getattr(msg, 't', None),
                "price": getattr(msg, 'p', None),
                "size": getattr(msg, 's', None),
                "exchange": getattr(msg, 'x', None),
                "conditions": getattr(msg, 'c', None),
                "trade_id": getattr(msg, 'i', None)
            }
            
        elif event_type == 'Q':  # Quote data
            return {
                "event_type": "quote",
                "symbol": getattr(msg, 'sym', getattr(msg, 'symbol', None)),
                "timestamp": getattr(msg, 't', None),
                "bid_price": getattr(msg, 'bp', None),
                "bid_size": getattr(msg, 'bs', None),
                "ask_price": getattr(msg, 'ap', None),
                "ask_size": getattr(msg, 'as', None),
                "bid_exchange": getattr(msg, 'bx', None),
                "ask_exchange": getattr(msg, 'ax', None)
            }
            
        elif event_type == 'status':  # Connection status
            logging.info(f"Status message: {getattr(msg, 'message', msg)}")
            return None
            
        else:
            # Unknown event type, but still try to capture basic info
            return {
                "event_type": event_type,
                "symbol": getattr(msg, 'sym', getattr(msg, 'symbol', 'unknown')),
                "timestamp": getattr(msg, 't', getattr(msg, 's', None)),
                "raw_data": str(msg)
            }
    
    # Handle status messages or connection events
    elif hasattr(msg, 'status') or 'status' in str(msg).lower():
        logging.info(f"Status/Connection message: {msg}")
        return None
    
    # Unknown message type - log for debugging
    else:
        logging.warning(f"Unknown message type: {type(msg)} - {msg}")
        return {
            "event_type": "unknown",
            "raw_data": str(msg),
            "timestamp": int(time.time() * 1000)  # Current timestamp in milliseconds
        }

def send_to_kafka(symbol, data):
    """
    Send processed data to Kafka
    """
    try:
        # Use symbol as key for partitioning
        producer.produce(
            topic=KAFKA_TOPIC,
            key=symbol,
            value=json.dumps(data, default=str),
            callback=delivery_report
        )
        
        # Trigger delivery reports periodically
        producer.poll(0)  # Non-blocking poll to trigger callbacks
        
    except Exception as e:
        logging.error(f"Error sending {symbol} data to Kafka: {e}")
        global kafka_error_count
        kafka_error_count += 1

def handle_msg(msgs):
    """
    Process messages from Polygon WebSocket and send to Kafka (throttled)
    """
    global message_count, processed_count
    
    for msg in msgs:
        message_count += 1
        
        # Show first few messages for debugging
        if message_count <= 3:
            logging.info(f"Processing message #{message_count}: {type(msg)}")
        
        # Process the message
        processed_data = process_polygon_message(msg)
        
        # Skip status messages or invalid data
        if processed_data is None:
            continue
        
        # Get symbol and check if we should process it (throttling)
        symbol = processed_data.get('symbol', 'unknown')
        
        # Only process if enough time has passed for this symbol
        if not should_process_symbol(symbol):
            continue  # Skip this update for this symbol
        
        processed_count += 1
        
        # Send to Kafka
        send_to_kafka(symbol, processed_data)
        
        # Log the data locally as well (optional)
        logging.info(f"updates:{symbol} - {json.dumps(processed_data, default=str)}")
        
        # Print statistics every 20 processed messages
        if processed_count % 20 == 0:
            logging.info(f"Stats: Received {message_count} messages, Published {processed_count} updates")
            logging.info(f"Kafka: {kafka_success_count} success, {kafka_error_count} errors")
            logging.info(f"Active symbols: {list(last_update_time.keys())}")

def create_kafka_producer():
    """
    Create and configure Kafka producer
    """
    kafka_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
        'allow.auto.create.topics': 'true',
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,   # Retry failed sends
        'batch.size': 16384,  # Batch size for efficiency
        'linger.ms': 10,      # Wait up to 10ms to batch messages
    }
    
    try:
        producer = Producer(kafka_config)
        logging.info("Successfully created Kafka producer")
        return producer
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        raise

def stats_reporter():
    """Background thread to report statistics every 30 seconds"""
    while True:
        time.sleep(30)
        runtime = datetime.now() - start_time
        logging.info(f"=== 30-Second Stats Report ===")
        logging.info(f"Runtime: {runtime}")
        logging.info(f"Messages received: {message_count}")
        logging.info(f"Messages published: {processed_count}")
        logging.info(f"Kafka success: {kafka_success_count}")
        logging.info(f"Kafka errors: {kafka_error_count}")
        logging.info(f"Active symbols: {len(last_update_time)}")
        logging.info("============================")

def connection_monitor():
    """Monitor connection health and log status"""
    last_message_count = 0
    
    while True:
        time.sleep(15)  # Check every 15 seconds
        
        # Check if we've received new messages
        if message_count > last_message_count:
            logging.info(f"Connection healthy: {message_count - last_message_count} new messages")
            last_message_count = message_count
        else:
            logging.warning(f"No new messages in last 15 seconds (total: {message_count})")
            if message_count > 0:
                logging.warning("This may indicate end of market data or connection issues")

def main():
    """Main function"""
    global producer, start_time
    start_time = datetime.now()
    
    logging.info("Starting Polygon.io to Kafka Producer")
    logging.info(f"Start time: {start_time}")
    logging.info(f"Symbols: {SYMBOLS}")
    logging.info(f"Kafka topic: {KAFKA_TOPIC}")
    logging.info(f"Update interval: {UPDATE_INTERVAL} seconds")

    # Check market hours
    import pytz
    eastern = pytz.timezone('US/Eastern')
    current_et = datetime.now(eastern)
    logging.info(f"Current Eastern Time: {current_et}")
    logging.info(f"Is weekday: {current_et.weekday() < 5}")
    logging.info(f"Hour: {current_et.hour} (market open: 9:30-16:00)")
    
    # Validate configuration
    if not POLYGON_API_KEY:
        logging.error("POLYGON_API_KEY not set")
        exit(1)
    
    if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET]):
        logging.error("Kafka configuration incomplete")
        logging.error(f"KAFKA_BOOTSTRAP_SERVERS: {'SET' if KAFKA_BOOTSTRAP_SERVERS else 'MISSING'}")
        logging.error(f"KAFKA_API_KEY: {'SET' if KAFKA_API_KEY else 'MISSING'}")
        logging.error(f"KAFKA_API_SECRET: {'SET' if KAFKA_API_SECRET else 'MISSING'}")
        exit(1)

    # Test Kafka connection first
    logging.info("Testing Kafka connection...")
    try:
        producer = create_kafka_producer()
        
        # Send a test message
        test_data = {"test": "connection", "timestamp": datetime.now().isoformat()}
        producer.produce(
            topic=KAFKA_TOPIC,
            key="test",
            value=json.dumps(test_data),
            callback=delivery_report
        )
        producer.flush(timeout=10)  # Wait for test message
        
        if kafka_success_count > 0:
            logging.info("SUCCESS: Kafka connection test successful")
        else:
            logging.error("FAILED: Kafka connection test failed - check your credentials")
            logging.error("Check your .env file and Kafka cluster settings")
            exit(1)
            
    except Exception as e:
        logging.error(f"Failed to initialize Kafka producer: {e}")
        exit(1)

    try:
        # Start background stats reporter
        stats_thread = threading.Thread(target=stats_reporter, daemon=True)
        stats_thread.start()
        
        # Start connection monitor
        monitor_thread = threading.Thread(target=connection_monitor, daemon=True)
        monitor_thread.start()
        
        # Create WebSocket client with reconnection handling
        logging.info("Creating Polygon WebSocket client...")
        
        # Try different subscription strategies based on market hours
        if current_et.weekday() >= 5:  # Weekend
            logging.warning("It's weekend - limited data expected")
            subscriptions = [f"A.AAPL", f"A.TSLA"]  # Just 2 most active stocks
        elif current_et.hour < 9 or current_et.hour >= 16:  # Outside market hours
            logging.warning("Outside market hours - limited data expected")
            subscriptions = [f"A.AAPL", f"A.TSLA", f"A.NVDA"]  # 3 most active
        else:
            logging.info("Market is open - using moderate subscription")
            subscriptions = [f"A.{symbol}" for symbol in SYMBOLS[:5]]  # Start with 5 symbols
            
        # Add reconnection loop with continuous operation
        retry_count = 0
        
        while True:  # Keep running indefinitely
            try:
                client = WebSocketClient(
                    api_key=POLYGON_API_KEY,
                    feed='delayed.polygon.io',  # Use delayed feed for stability
                    market="stocks",
                    subscriptions=subscriptions
                )

                retry_count += 1
                logging.info(f"Connecting to Polygon.io WebSocket (attempt {retry_count})...")
                logging.info(f"Subscriptions: {subscriptions}")
                if retry_count == 1:
                    logging.info("Press Ctrl+C to stop...")
                
                # Run the WebSocket client
                client.run(handle_msg)
                
                # If we get here, connection ended - prepare to reconnect
                logging.warning("WebSocket connection ended - will reconnect in 5 seconds...")
                time.sleep(5)  # Wait before reconnecting
                
            except KeyboardInterrupt:
                logging.info("Manual shutdown requested")
                break
            except Exception as e:
                logging.error(f"WebSocket connection failed (attempt {retry_count}): {e}")
                
                # Exponential backoff with max 60 seconds
                wait_time = min(retry_count * 5, 60)
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        # Summary of why connection might have ended
        logging.info("Connection ended. Possible reasons:")
        if current_et.weekday() >= 5:
            logging.info("- Weekend: Markets are closed")
        elif current_et.hour < 9:
            logging.info("- Pre-market: Limited activity")
        elif current_et.hour >= 16:
            logging.info("- After-hours: Limited activity")
        else:
            logging.info("- During market hours: Check API limits or connection stability")
            
    finally:
        # Clean shutdown
        logging.info("Shutting down...")
        if producer:
            producer.flush(timeout=10)  # Wait for pending messages
        
        runtime = datetime.now() - start_time
        logging.info(f"Final stats - Runtime: {runtime}")
        logging.info(f"Total received: {message_count}")
        logging.info(f"Total published: {processed_count}")
        logging.info(f"Kafka success: {kafka_success_count}")
        logging.info(f"Kafka errors: {kafka_error_count}")
        logging.info("Shutdown complete")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
        if producer:
            producer.flush(timeout=10)
        
        runtime = datetime.now() - start_time
        logging.info(f"Final stats - Runtime: {runtime}")
        logging.info(f"Total received: {message_count}, Published: {processed_count}")
        logging.info(f"Kafka: {kafka_success_count} success, {kafka_error_count} errors")