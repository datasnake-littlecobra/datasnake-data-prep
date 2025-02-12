from datetime import datetime, timedelta

class DataFrameCache:
    def __init__(self, expiration_minutes=60):
        self.cache = {}  # Dictionary to store DataFrames
        self.expiration_minutes = expiration_minutes  # Cache expiry time

    def set(self, key, df):
        """Store DataFrame in cache with timestamp"""
        self.cache[key] = {
            "data": df,
            "timestamp": datetime.now()
        }

    def get(self, key):
        """Retrieve DataFrame from cache if it exists and is not expired"""
        if key in self.cache:
            entry = self.cache[key]
            # Check if expired
            if (datetime.now() - entry["timestamp"]) < timedelta(minutes=self.expiration_minutes):
                return entry["data"]
            else:
                self.invalidate(key)  # Remove expired entry
        return None  # Return None if not found or expired

    def invalidate(self, key):
        """Remove a specific key from the cache"""
        if key in self.cache:
            del self.cache[key]

    def clear(self):
        """Clear the entire cache"""
        self.cache.clear()

# Initialize cache with 60-minute expiry
cache = DataFrameCache()
