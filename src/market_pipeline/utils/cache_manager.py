"""
Cache manager for intermediate results to improve performance.
Supports both memory and disk-based caching.
"""

import os
import pickle
import hashlib
import logging
import tempfile
from typing import Any, Optional, Dict, Callable
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd


logger = logging.getLogger(__name__)


class CacheManager:
    """
    A cache manager that supports both memory and disk-based caching.
    Useful for storing intermediate computation results.
    """

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        max_memory_items: int = 100,
        default_ttl_hours: int = 24,
    ):
        """
        Initialize cache manager.

        Args:
            cache_dir: Directory for disk cache. If None, uses system temp.
            max_memory_items: Maximum items to keep in memory cache.
            default_ttl_hours: Default time-to-live for cache entries in hours.
        """
        self.max_memory_items = max_memory_items
        self.default_ttl_hours = default_ttl_hours

        # Memory cache
        self._memory_cache: Dict[str, Dict[str, Any]] = {}

        # Disk cache directory
        if cache_dir is None:
            cache_dir = os.path.join(tempfile.gettempdir(), "stock_analysis_cache")

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Cache manager initialized with disk cache at: {self.cache_dir}")

    def _generate_key(self, key_data: Any) -> str:
        """Generate a hash key from arbitrary data."""
        if isinstance(key_data, str):
            return hashlib.md5(key_data.encode()).hexdigest()
        elif isinstance(key_data, (list, tuple)):
            combined = "_".join(str(item) for item in key_data)
            return hashlib.md5(combined.encode()).hexdigest()
        elif isinstance(key_data, dict):
            combined = "_".join(f"{k}:{v}" for k, v in sorted(key_data.items()))
            return hashlib.md5(combined.encode()).hexdigest()
        else:
            return hashlib.md5(str(key_data).encode()).hexdigest()

    def _is_expired(self, timestamp: datetime, ttl_hours: int) -> bool:
        """Check if a cache entry has expired."""
        expiry_time = timestamp + timedelta(hours=ttl_hours)
        return datetime.now() > expiry_time

    def _cleanup_memory_cache(self):
        """Remove expired entries and enforce size limits."""
        # Remove expired entries
        expired_keys = []
        for key, entry in self._memory_cache.items():
            if self._is_expired(entry["timestamp"], entry["ttl_hours"]):
                expired_keys.append(key)

        for key in expired_keys:
            del self._memory_cache[key]

        # Enforce size limit (remove oldest entries)
        if len(self._memory_cache) > self.max_memory_items:
            # Sort by timestamp and remove oldest
            sorted_items = sorted(
                self._memory_cache.items(), key=lambda x: x[1]["timestamp"]
            )

            items_to_remove = len(self._memory_cache) - self.max_memory_items
            for key, _ in sorted_items[:items_to_remove]:
                del self._memory_cache[key]

    def get(self, key: Any, use_disk: bool = True) -> Optional[Any]:
        """
        Retrieve an item from cache.

        Args:
            key: Cache key (can be string, list, dict, etc.)
            use_disk: Whether to check disk cache if not in memory

        Returns:
            Cached value or None if not found/expired
        """
        cache_key = self._generate_key(key)

        # Check memory cache first
        if cache_key in self._memory_cache:
            entry = self._memory_cache[cache_key]
            if not self._is_expired(entry["timestamp"], entry["ttl_hours"]):
                logger.debug(f"Cache hit (memory): {cache_key}")
                return entry["data"]
            else:
                # Remove expired entry
                del self._memory_cache[cache_key]

        # Check disk cache
        if use_disk:
            disk_path = self.cache_dir / f"{cache_key}.pkl"
            if disk_path.exists():
                try:
                    with open(disk_path, "rb") as f:
                        entry = pickle.load(f)

                    if not self._is_expired(entry["timestamp"], entry["ttl_hours"]):
                        logger.debug(f"Cache hit (disk): {cache_key}")
                        # Load back into memory cache
                        self._memory_cache[cache_key] = entry
                        self._cleanup_memory_cache()
                        return entry["data"]
                    else:
                        # Remove expired file
                        disk_path.unlink()
                except Exception as e:
                    logger.warning(f"Error reading disk cache {cache_key}: {e}")

        logger.debug(f"Cache miss: {cache_key}")
        return None

    def put(
        self,
        key: Any,
        data: Any,
        ttl_hours: Optional[int] = None,
        use_disk: bool = True,
    ) -> None:
        """
        Store an item in cache.

        Args:
            key: Cache key
            data: Data to cache
            ttl_hours: Time to live in hours (uses default if None)
            use_disk: Whether to also store on disk
        """
        cache_key = self._generate_key(key)
        ttl_hours = ttl_hours or self.default_ttl_hours

        entry = {"data": data, "timestamp": datetime.now(), "ttl_hours": ttl_hours}

        # Store in memory cache
        self._memory_cache[cache_key] = entry
        self._cleanup_memory_cache()

        # Store in disk cache
        if use_disk:
            try:
                disk_path = self.cache_dir / f"{cache_key}.pkl"
                with open(disk_path, "wb") as f:
                    pickle.dump(entry, f)
                logger.debug(f"Cached to disk: {cache_key}")
            except Exception as e:
                logger.warning(f"Error writing disk cache {cache_key}: {e}")

        logger.debug(f"Cached in memory: {cache_key}")

    def cached_function(self, ttl_hours: Optional[int] = None, use_disk: bool = True):
        """
        Decorator to cache function results.

        Args:
            ttl_hours: Time to live for cached results
            use_disk: Whether to use disk caching

        Returns:
            Decorator function
        """

        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                # Create cache key from function name and arguments
                key_data = {"func": func.__name__, "args": args, "kwargs": kwargs}

                # Check cache first
                result = self.get(key_data, use_disk)
                if result is not None:
                    logger.debug(f"Using cached result for {func.__name__}")
                    return result

                # Compute and cache result
                logger.debug(f"Computing and caching result for {func.__name__}")
                result = func(*args, **kwargs)
                self.put(key_data, result, ttl_hours, use_disk)
                return result

            return wrapper

        return decorator

    def clear_memory(self) -> None:
        """Clear all memory cache."""
        self._memory_cache.clear()
        logger.info("Memory cache cleared")

    def clear_disk(self) -> None:
        """Clear all disk cache."""
        try:
            for cache_file in self.cache_dir.glob("*.pkl"):
                cache_file.unlink()
            logger.info("Disk cache cleared")
        except Exception as e:
            logger.error(f"Error clearing disk cache: {e}")

    def clear_all(self) -> None:
        """Clear both memory and disk cache."""
        self.clear_memory()
        self.clear_disk()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        disk_files = len(list(self.cache_dir.glob("*.pkl")))

        # Calculate disk cache size
        disk_size = sum(f.stat().st_size for f in self.cache_dir.glob("*.pkl"))

        return {
            "memory_items": len(self._memory_cache),
            "disk_items": disk_files,
            "disk_size_mb": disk_size / (1024 * 1024),
            "cache_dir": str(self.cache_dir),
        }

    def cleanup_expired(self) -> None:
        """Remove all expired cache entries."""
        # Clean memory cache
        self._cleanup_memory_cache()

        # Clean disk cache
        expired_files = []
        for cache_file in self.cache_dir.glob("*.pkl"):
            try:
                with open(cache_file, "rb") as f:
                    entry = pickle.load(f)

                if self._is_expired(entry["timestamp"], entry["ttl_hours"]):
                    expired_files.append(cache_file)
            except Exception as e:
                logger.warning(f"Error checking cache file {cache_file}: {e}")
                expired_files.append(cache_file)  # Remove corrupted files

        for cache_file in expired_files:
            try:
                cache_file.unlink()
            except Exception as e:
                logger.warning(f"Error removing expired cache file {cache_file}: {e}")

        logger.info(f"Cleaned up {len(expired_files)} expired disk cache entries")


# Global cache instance
_global_cache = None


def get_cache() -> CacheManager:
    """Get the global cache instance."""
    global _global_cache
    if _global_cache is None:
        cache_dir = "/Users/tak/Markets/Stocks/Stock-Analysis/data/cache"
        _global_cache = CacheManager(cache_dir=cache_dir)
    return _global_cache


def cache_dataframe(key: Any, df: pd.DataFrame, ttl_hours: int = 24) -> None:
    """
    Cache a pandas DataFrame.

    Args:
        key: Cache key
        df: DataFrame to cache
        ttl_hours: Time to live in hours
    """
    cache = get_cache()
    # Convert DataFrame to a more efficient format for caching
    cache_data = {
        "data": df.to_dict("records"),
        "index": df.index.tolist(),
        "columns": df.columns.tolist(),
    }
    cache.put(key, cache_data, ttl_hours)


def get_cached_dataframe(key: Any) -> Optional[pd.DataFrame]:
    """
    Retrieve a cached pandas DataFrame.

    Args:
        key: Cache key

    Returns:
        Cached DataFrame or None if not found
    """
    cache = get_cache()
    cache_data = cache.get(key)

    if cache_data is not None:
        try:
            df = pd.DataFrame(cache_data["data"])
            if cache_data["index"]:
                df.index = cache_data["index"]
            return df
        except Exception as e:
            logger.warning(f"Error reconstructing DataFrame from cache: {e}")

    return None


def cache_stock_data(
    stock_code: str, date_range: str, data: Any, ttl_hours: int = 6
) -> None:
    """
    Cache stock-specific data.

    Args:
        stock_code: Stock code
        date_range: Date range string
        data: Data to cache
        ttl_hours: Time to live in hours
    """
    key = f"stock_data_{stock_code}_{date_range}"
    get_cache().put(key, data, ttl_hours)


def get_cached_stock_data(stock_code: str, date_range: str) -> Optional[Any]:
    """
    Retrieve cached stock-specific data.

    Args:
        stock_code: Stock code
        date_range: Date range string

    Returns:
        Cached data or None if not found
    """
    key = f"stock_data_{stock_code}_{date_range}"
    return get_cache().get(key)
