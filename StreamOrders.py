import csv
import os
import sys
from typing import Dict, Any
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


def stream_orders(csv_file: str, stream_name: str) -> None:
    """
    Read orders from a CSV file and stream them to Redis.
    
    Args:
        csv_file: Path to the CSV file containing order data
        stream_name: Name of the Redis stream
    """
    redis_client = None
    try:
        # Create Redis connection
        redis_client = create_redis_connection()
        print(f"✅ Successfully connected to Redis. Starting to stream data to '{stream_name}'...")
        
        # Process CSV file
        with open(csv_file, encoding='utf-8-sig') as csvf:
            csv_reader = csv.DictReader(csvf)
            total_processed = 0
            
            for row in csv_reader:
                try:
                    # Convert all values to strings as Redis Streams require string values
                    processed_row = {k: str(v) if v is not None else '' 
                                   for k, v in row.items()}
                    
                    # Add to Redis stream
                    redis_client.xadd(stream_name, processed_row)
                    total_processed += 1
                    
                    # Print progress every 1000 records
                    if total_processed % 1000 == 0:
                        print(f"Processed {total_processed} records...")
                        
                except Exception as e:
                    print(f"⚠️ Error processing row {total_processed + 1}: {e}", file=sys.stderr)
                    continue
        
        print(f"✅ Successfully processed {total_processed} records to Redis stream: {stream_name}")
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {csv_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ An error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if redis_client:
            redis_client.close()


if __name__ == "__main__":
    # Configuration
    CSV_FILE = "OnlineRetail.csv"
    STREAM_NAME = "orders"
    
    # Start streaming
    stream_orders(CSV_FILE, STREAM_NAME)
