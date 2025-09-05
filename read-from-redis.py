import redis
import os
from dotenv import load_dotenv
from typing import Optional


def create_redis_connection() -> redis.Redis:
    """
    Create and return a Redis connection using environment variables.
    
    Returns:
        redis.Redis: Redis client instance
    """
    try:
        # Load environment variables from .env file if it exists
        load_dotenv()
        
        # Get Redis connection details from environment variables
        redis_host = os.getenv('REDIS_HOST')
        redis_port = int(os.getenv('REDIS_PORT', '19536'))
        redis_username = os.getenv('REDIS_USERNAME', 'default')
        redis_password = os.getenv('REDIS_PASSWORD')
        redis_db = int(os.getenv('REDIS_DB', '0'))
        
        if not all([redis_host, redis_password]):
            raise ValueError("Missing required Redis connection parameters in environment variables")
        
        # Create Redis connection
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            username=redis_username,
            password=redis_password,
            db=redis_db,
            decode_responses=True,
            socket_connect_timeout=5,  # 5 seconds timeout
            health_check_interval=30,   # Check connection health every 30 seconds
        )
        
        # Test the connection
        if not redis_client.ping():
            raise ConnectionError("Failed to ping Redis server")
            
        return redis_client
        
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        raise


def get_key(redis_client: redis.Redis, key: str) -> Optional[str]:
    """Get a value from Redis by key."""
    try:
        return redis_client.get(key)
    except redis.RedisError as e:
        print(f"Error getting key '{key}': {e}")
        return None


def set_key(redis_client: redis.Redis, key: str, value: str) -> bool:
    """Set a key-value pair in Redis."""
    try:
        return redis_client.set(key, value)
    except redis.RedisError as e:
        print(f"Error setting key '{key}': {e}")
        return False


def main():
    """Main function to demonstrate Redis operations."""
    try:
        # Create Redis connection
        redis_client = create_redis_connection()
        print("✅ Successfully connected to Redis!")
        
        # Example usage
        key = "mykey"
        value = "myvalue"
        
        # Set a key
        if set_key(redis_client, key, value):
            print(f"✅ Successfully set key: {key} = {value}")
        
        # Get the key
        retrieved_value = get_key(redis_client, key)
        if retrieved_value is not None:
            print(f"✅ Retrieved value for key '{key}': {retrieved_value}")
        
        # Example of getting a non-existent key
        non_existent = get_key(redis_client, "nonexistent_key")
        if non_existent is None:
            print("ℹ️ Key 'nonexistent_key' does not exist")
        
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return 1
    finally:
        # Close the connection when done
        if 'redis_client' in locals():
            redis_client.close()
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
