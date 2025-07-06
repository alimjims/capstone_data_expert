import json
import os
import time
import threading
import logging
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('fake_stock_kafka.log'), logging.StreamHandler()]
)

# Load environment variables
load_dotenv()

# Configuration
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

# Fake stock data state
class StockSimulator:
    def __init__(self):
        # Initialize realistic starting prices for each symbol
        self.prices = {
            "AAPL": 175.00,
            "MSFT": 380.00,
            "GOOGL": 135.00,
            "AMZN": 145.00,
            "TSLA": 250.00,
            "NVDA": 900.00,
            "META": 320.00,
            "NFLX": 450.00,
            "RBLX": 35.00,
            "COIN": 85.00
        }
        
        # Track daily open prices for each symbol
        self.daily_opens = self.prices.copy()
        
        # Track high/low for the day
        self.daily_highs = self.prices.copy()
        self.daily_lows = self.prices.copy()
        
        # Volume tracking
        self.daily_volumes = {symbol: 0 for symbol in SYMBOLS}
        
        # Trade count tracking
        self.trade_counts = {symbol: 0 for symbol in SYMBOLS}

    def generate_price_movement(self, symbol, current_price):
        """Generate realistic price movement"""
        # Different volatility for different stocks
        volatility_map = {
            "AAPL": 0.02,    # 2% max move
            "MSFT": 0.015,   # 1.5% max move
            "GOOGL": 0.025,  # 2.5% max move
            "AMZN": 0.03,    # 3% max move
            "TSLA": 0.05,    # 5% max move (more volatile)
            "NVDA": 0.04,    # 4% max move
            "META": 0.035,   # 3.5% max move
            "NFLX": 0.03,    # 3% max move
            "RBLX": 0.06,    # 6% max move (more volatile)
            "COIN": 0.07     # 7% max move (most volatile)
        }
        
        volatility = volatility_map.get(symbol, 0.02)
        
        # Generate random price movement (more likely to be small)
        change_percent = random.gauss(0, volatility / 3)  # Normal distribution
        change_percent = max(-volatility, min(volatility, change_percent))  # Cap at max volatility
        
        new_price = current_price * (1 + change_percent)
        new_price = max(0.01, new_price)  # Ensure price doesn't go negative
        
        return round(new_price, 2)

    def generate_volume(self, symbol):
        """Generate realistic volume for a trade"""
        base_volumes = {
            "AAPL": 1000,
            "MSFT": 800,
            "GOOGL": 600,
            "AMZN": 700,
            "TSLA": 1500,
            "NVDA": 1200,
            "META": 900,
            "NFLX": 500,
            "RBLX": 400,
            "COIN": 300
        }
        
        base = base_volumes.get(symbol, 500)
        # Random volume between 50% and 200% of base
        volume = random.randint(base // 2, base * 2)
        return volume

    def generate_aggregate_data(self, symbol):
        """Generate OHLCV aggregate data matching TimescaleDB schema"""
        current_price = self.prices[symbol]
        
        # Generate new price
        new_price = self.generate_price_movement(symbol, current_price)
        
        # For aggregate data, we simulate a 1-minute bar
        # Open = previous close, High/Low around the range, Close = new price
        open_price = current_price
        close_price = new_price
        
        # Generate high/low for this minute
        price_range = abs(new_price - current_price)
        high_price = max(open_price, close_price) + random.uniform(0, price_range * 0.5)
        low_price = min(open_price, close_price) - random.uniform(0, price_range * 0.5)
        
        # Ensure high >= low
        high_price = max(high_price, low_price + 0.01)
        
        # Round prices
        open_price = round(open_price, 2)
        high_price = round(high_price, 2)
        low_price = round(low_price, 2)
        close_price = round(close_price, 2)
        
        # Generate volume for this minute
        volume = self.generate_volume(symbol)
        
        # Update tracking
        self.prices[symbol] = close_price
        self.daily_highs[symbol] = max(self.daily_highs[symbol], high_price)
        self.daily_lows[symbol] = min(self.daily_lows[symbol], low_price)
        self.daily_volumes[symbol] += volume
        self.trade_counts[symbol] += random.randint(10, 50)
        
        # Calculate VWAP (simplified)
        vwap = (high_price + low_price + close_price) / 3
        
        return {
            "event_type": "aggregate",
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "price": close_price,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,
            "vwap": round(vwap, 2),
            "accumulated_volume": self.daily_volumes[symbol],
            "trade_count": self.trade_counts[symbol]
        }



# Global simulator instance
simulator = StockSimulator()

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

def generate_fake_market_data():
    """
    Generate fake market data for all symbols
    """
    global message_count, processed_count
    
    for symbol in SYMBOLS:
        # Check throttling
        if not should_process_symbol(symbol):
            continue
        
        message_count += 1
        
        # Generate only aggregate data
        data = simulator.generate_aggregate_data(symbol)
        
        processed_count += 1
        
        # Send to Kafka
        send_to_kafka(symbol, data)
        
        # Log the data locally
        price_display = f"${data['price']:.2f} (O:{data['open_price']:.2f} H:{data['high_price']:.2f} L:{data['low_price']:.2f} C:{data['close_price']:.2f})"
        logging.info(f"Generated aggregate for {symbol}: {price_display}")
        
        # Print statistics every 20 processed messages
        if processed_count % 20 == 0:
            logging.info(f"Stats: Generated {message_count} messages, Published {processed_count} updates")
            logging.info(f"Kafka: {kafka_success_count} success, {kafka_error_count} errors")
            logging.info(f"Current prices: {[(s, f'${p:.2f}') for s, p in list(simulator.prices.items())[:3]]}")

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
        logging.info(f"Messages generated: {message_count}")
        logging.info(f"Messages published: {processed_count}")
        logging.info(f"Kafka success: {kafka_success_count}")
        logging.info(f"Kafka errors: {kafka_error_count}")
        logging.info(f"Current prices: {[(s, f'${p:.2f}') for s, p in simulator.prices.items()]}")
        logging.info("============================")

def market_data_generator():
    """Main loop to generate fake market data"""
    logging.info("Starting fake market data generation...")
    
    while True:
        try:
            # Generate data for all symbols
            generate_fake_market_data()
            
            # Wait a bit before next generation cycle
            time.sleep(random.uniform(0.5, 2.0))  # Random delay between 0.5-2 seconds
            
        except KeyboardInterrupt:
            logging.info("Market data generation stopped by user")
            break
        except Exception as e:
            logging.error(f"Error in market data generation: {e}")
            time.sleep(1)  # Wait before retrying

def main():
    """Main function"""
    global producer, start_time
    start_time = datetime.now()
    
    logging.info("Starting Fake Stock Data Generator for Kafka Testing")
    logging.info(f"Start time: {start_time}")
    logging.info(f"Symbols: {SYMBOLS}")
    logging.info(f"Kafka topic: {KAFKA_TOPIC}")
    logging.info(f"Update interval: {UPDATE_INTERVAL} seconds")
    logging.info("🚀 FAKE DATA MODE - Perfect for testing during off-market hours!")
    
    # Show initial prices
    logging.info("Starting prices:")
    for symbol, price in simulator.prices.items():
        logging.info(f"  {symbol}: ${price:.2f}")
    
    # Validate Kafka configuration
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
        test_data = {"test": "fake_data_connection", "timestamp": datetime.now().isoformat()}
        producer.produce(
            topic=KAFKA_TOPIC,
            key="test",
            value=json.dumps(test_data),
            callback=delivery_report
        )
        producer.flush(timeout=10)  # Wait for test message
        
        if kafka_success_count > 0:
            logging.info("✅ SUCCESS: Kafka connection test successful")
        else:
            logging.error("❌ FAILED: Kafka connection test failed - check your credentials")
            exit(1)
            
    except Exception as e:
        logging.error(f"Failed to initialize Kafka producer: {e}")
        exit(1)

    try:
        # Start background stats reporter
        stats_thread = threading.Thread(target=stats_reporter, daemon=True)
        stats_thread.start()
        
        logging.info("🎯 Starting fake market data generation...")
        logging.info("Press Ctrl+C to stop...")
        
        # Start generating fake market data
        market_data_generator()
            
    except KeyboardInterrupt:
        logging.info("Manual shutdown requested")
        
    finally:
        # Clean shutdown
        logging.info("Shutting down...")
        if producer:
            producer.flush(timeout=10)  # Wait for pending messages
        
        runtime = datetime.now() - start_time
        logging.info(f"📊 Final stats - Runtime: {runtime}")
        logging.info(f"Total generated: {message_count}")
        logging.info(f"Total published: {processed_count}")
        logging.info(f"Kafka success: {kafka_success_count}")
        logging.info(f"Kafka errors: {kafka_error_count}")
        logging.info("Final prices:")
        for symbol, price in simulator.prices.items():
            change = price - simulator.daily_opens[symbol]
            change_pct = (change / simulator.daily_opens[symbol]) * 100
            logging.info(f"  {symbol}: ${price:.2f} ({change:+.2f}, {change_pct:+.1f}%)")
        logging.info("✅ Shutdown complete")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
        if producer:
            producer.flush(timeout=10)
        
        runtime = datetime.now() - start_time
        logging.info(f"Final stats - Runtime: {runtime}")
        logging.info(f"Total generated: {message_count}, Published: {processed_count}")
        logging.info(f"Kafka: {kafka_success_count} success, {kafka_error_count} errors")