import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from polygon import WebSocketClient



# Load environment variables
load_dotenv()

# Configuration
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

print("Starting Polygon WebSocket data stream...")
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX", "RBLX", "COIN" ]

# Counters
message_count = 0
processed_count = 0

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
            print(f"Status message: {getattr(msg, 'message', msg)}")
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
        print(f"Status/Connection message: {msg}")
        return None
    
    # Unknown message type - log for debugging
    else:
        print(f"Unknown message type: {type(msg)} - {msg}")
        return {
            "event_type": "unknown",
            "raw_data": str(msg),
            "timestamp": int(time.time() * 1000)  # Current timestamp in milliseconds
        }

def handle_msg(msgs):
    """
    Process messages from Polygon WebSocket and display them
    """
    global message_count, processed_count
    
    for msg in msgs:
        message_count += 1
        
        # Show first few messages for debugging
        if message_count <= 5:
            print(f"Processing message #{message_count}: {type(msg)} - {msg}")
        
        # Process the message
        processed_data = process_polygon_message(msg)
        
        # Skip status messages or invalid data
        if processed_data is None:
            continue
        
        processed_count += 1
        
        # Display the processed data
        symbol = processed_data.get('symbol', 'unknown')
        event_type = processed_data.get('event_type', 'unknown')
        
        # Print formatted data similar to your desired output
        print(f"updates:{symbol} - {json.dumps(processed_data, default=str)}")
        
        # Print statistics every 50 processed messages
        if processed_count % 50 == 0:
            print(f"\n--- Stats: Received {message_count} messages, Processed {processed_count} messages ---")
            print(f"Time: {datetime.now()}")
            print()

def main():
    """Main function"""
    print("Starting Polygon.io WebSocket data stream (Local Processing Only)")
    print(f"Current time: {datetime.now()}")

    # Validate configuration
    if not POLYGON_API_KEY:
        print("Error: POLYGON_API_KEY not set")
        exit(1)

    try:
        # Create WebSocket client - start with delayed feed for reliability
        client = WebSocketClient(
            api_key=POLYGON_API_KEY,
            feed='delayed.polygon.io',  # Use delayed feed for stable connection
            market="stocks",
            subscriptions=[f"A.{symbol}" for symbol in SYMBOLS]
        )

        print("Connecting to Polygon.io WebSocket...")
        print("Subscribed to aggregate and trade data for major stocks")
        print("Data will be displayed in console (no Kafka)")
        print("Press Ctrl+C to stop...")
        
        # Run the WebSocket client
        client.run(handle_msg)

    except Exception as e:
        print(f"Error with delayed feed: {e}")
        
        # Try real-time feed as fallback
        print("Attempting fallback to real-time feed...")
        try:
            client_realtime = WebSocketClient(
                api_key=POLYGON_API_KEY,
                feed='realtime.polygon.io',
                market="stocks",
                subscriptions=["A.AAPL", "A.MSFT", "A.GOOGL",]
            )
            
            print("Connected to real-time feed")
            client_realtime.run(handle_msg)
            
        except Exception as e2:
            print(f"Real-time feed also failed: {e2}")
            
    finally:
        # Clean shutdown
        print("Shutting down...")
        print(f"Final stats - Total received: {message_count}, Total processed: {processed_count}")
        print("Data stream stopped")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        print(f"Final stats - Total received: {message_count}, Total processed: {processed_count}")