import os
import sys
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import redis


def create_redis_connection() -> redis.Redis:
    """
    Create and return a Redis connection using environment variables.
    
    Returns:
        redis.Redis: Redis client instance
    """
    try:
        # Load environment variables from .env file
        load_dotenv()
        
        # Get Redis connection details from environment variables
        redis_host = os.getenv('REDIS_HOST')
        redis_port = int(os.getenv('REDIS_PORT', '19536'))
        redis_username = os.getenv('REDIS_USERNAME', 'default')
        redis_password = os.getenv('REDIS_PASSWORD')
        
        if not all([redis_host, redis_password]):
            raise ValueError("Missing required Redis connection parameters in .env file")
        
        # Create Redis connection
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            username=redis_username,
            password=redis_password,
            decode_responses=True,
            socket_connect_timeout=5,
            health_check_interval=30,
        )
        
        # Test the connection
        if not redis_client.ping():
            raise ConnectionError("Failed to ping Redis server")
            
        return redis_client
        
    except Exception as e:
        print(f"❌ Error connecting to Redis: {e}", file=sys.stderr)
        sys.exit(1)


def format_order(order_data: Dict[str, str]) -> str:
    """Format order data into a human-readable string."""
    try:
        # Try to parse any JSON strings in the values
        formatted = {}
        for k, v in order_data.items():
            try:
                formatted[k] = json.loads(v) if v.startswith('{') and v.endswith('}') else v
            except (json.JSONDecodeError, AttributeError):
                formatted[k] = v
                
        return json.dumps(formatted, indent=2)
    except Exception as e:
        print(f"⚠️ Error formatting order: {e}")
        return str(order_data)


def subscribe_to_orders(stream_name: str = 'orders', last_id: str = '$') -> None:
    """
    Subscribe to a Redis stream and process incoming orders.
    
    Args:
        stream_name: Name of the Redis stream to subscribe to
        last_id: ID to start reading from (default: '$' for new messages)
    """
    redis_client = None
    try:
        redis_client = create_redis_connection()
        print(f"✅ Successfully connected to Redis. Listening to stream: {stream_name}")
        print("Press Ctrl+C to exit...\n" + "="*50)
        
        while True:
            try:
                # Read from the stream, blocking for up to 1 second
                messages = redis_client.xread(
                    {stream_name: last_id},
                    count=1,
                    block=1000  # 1 second timeout
                )
                
                if not messages:
                    continue
                    
                for stream in messages:
                    stream_name, entries = stream
                    print(f"\n📦 New order(s) in stream: {stream_name}")
                    print("-" * 50)
                    
                    for entry_id, order_data in entries:
                        print(f"🆔 Entry ID: {entry_id}")
                        print(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print("📝 Order Details:")
                        print(format_order(order_data))
                        print("-" * 50)
                        
                        # Update the last ID to avoid reprocessing the same messages
                        last_id = entry_id
                        
            except redis.exceptions.ConnectionError as e:
                print(f"⚠️ Redis connection error: {e}. Reconnecting...")
                time.sleep(5)  # Wait before reconnecting
                redis_client = create_redis_connection()
                
            except KeyboardInterrupt:
                print("\n👋 Exiting...")
                break
                
            except Exception as e:
                print(f"⚠️ Error processing message: {e}", file=sys.stderr)
                time.sleep(1)  # Prevent tight loop on errors
                
    except Exception as e:
        print(f"❌ Fatal error: {e}", file=sys.stderr)
        
    finally:
        if redis_client:
            redis_client.close()


if __name__ == "__main__":
    # Configuration
    STREAM_NAME = "orders"
    
    # Start the subscriber
    subscribe_to_orders(STREAM_NAME)
