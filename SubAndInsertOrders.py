import os
import sys
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import redis
from pydantic import ValidationError
from Schema import Product, Order  # Assuming Schema.py contains these classes


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


def process_order(order_data: Dict[str, Any]) -> Optional[Order]:
    """
    Process raw order data and return a validated Order object.
    
    Args:
        order_data: Dictionary containing order data from Redis stream
        
    Returns:
        Order: Validated Order object if successful, None otherwise
    """
    try:
        # Create Product instance
        item = Product(
            StockCode=order_data.get('StockCode', ''),
            Description=order_data.get('Description', ''),
            UnitPrice=order_data.get('UnitPrice', '0')
        )

        # Correct for Pydantic date field
        invoice_date_str = order_data.get('InvoiceDate', '')
        try:
            invoice_dt = datetime.strptime(invoice_date_str, '%m/%d/%Y %H:%M')
            invoice_date = invoice_dt.date()  # strips time
        except (ValueError, TypeError):
            print(f"⚠️ Invalid date format: {invoice_date_str}")
            return None
        # Pass invoice_date (type: date) to Order
        return Order(
            InvoiceNo=order_data.get('InvoiceNo', ''),
            Item=item,
            Quantity=order_data.get('Quantity', '1'),
            InvoiceDate=invoice_date,  # now just the date!
            CustomerID=order_data.get('CustomerID', ''),
            Country=order_data.get('Country', '')
        )

    except ValidationError as e:
        print(f"⚠️ Validation error: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Error processing order: {e}")
        return None


def subscribe_and_insert_orders(stream_name: str = 'orders', last_id: str = '$') -> None:
    """
    Subscribe to a Redis stream and insert valid orders into the database.
    
    Args:
        stream_name: Name of the Redis stream to subscribe to
        last_id: ID to start reading from (default: '$' for new messages)
    """
    redis_client = None
    processed_count = 0
    
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
                    _, entries = stream
                    
                    for entry_id, order_data in entries:
                        print(f"\n📦 Processing order: {entry_id}")
                        print("-" * 50)
                        
                        # Process and validate the order
                        order = process_order(order_data)
                        
                        if order:
                            try:
                                # Save the order to the database
                                order_key = order.key()
                                order.save()
                                processed_count += 1
                                print(f"✅ Successfully saved order: {order_key}")
                                print(f"📊 Total orders processed: {processed_count}")
                            except Exception as e:
                                print(f"⚠️ Error saving order: {e}")
                        
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
        print(f"\n📊 Total orders processed in this session: {processed_count}")


if __name__ == "__main__":

    # Configuration
    STREAM_NAME = "orders"
    
    # Start the subscriber and order processor
    subscribe_and_insert_orders(STREAM_NAME)
