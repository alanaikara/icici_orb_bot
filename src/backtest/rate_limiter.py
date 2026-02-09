import time
import logging
from datetime import date

logger = logging.getLogger("ICICI_ORB_Bot")


class RateLimiter:
    """
    Enforces two rate limits for the Breeze API:
    - Per-minute: max calls per 60-second sliding window
    - Per-day: max calls per calendar day (persisted in DB)
    """

    def __init__(self, calls_per_minute=95, calls_per_day=4900, db=None):
        """
        Args:
            calls_per_minute: Max API calls per 60-second window (default 95, safety margin from 100)
            calls_per_day: Max API calls per calendar day (default 4900, safety margin from 5000)
            db: BacktestDatabase instance for persisting daily call count
        """
        self.calls_per_minute = calls_per_minute
        self.calls_per_day = calls_per_day
        self.db = db

        # In-memory sliding window for per-minute tracking
        self._call_timestamps = []

    def can_proceed(self):
        """
        Check if we can make another API call without exceeding limits.
        Returns (bool, reason_string).
        """
        # Check daily limit from database
        today_str = date.today().isoformat()
        daily_calls = self.db.get_daily_api_calls(today_str)
        if daily_calls >= self.calls_per_day:
            return False, f"Daily limit reached: {daily_calls}/{self.calls_per_day}"

        # Check per-minute limit from in-memory timestamps
        now = time.time()
        self._call_timestamps = [t for t in self._call_timestamps if now - t < 60]
        if len(self._call_timestamps) >= self.calls_per_minute:
            return False, f"Minute limit reached: {len(self._call_timestamps)}/{self.calls_per_minute}"

        return True, "OK"

    def wait_if_needed(self):
        """
        Block until an API call is allowed.
        Returns True if ready to proceed, False if daily limit hit (caller should stop).
        """
        while True:
            can, reason = self.can_proceed()
            if can:
                return True

            if "Daily limit" in reason:
                logger.warning(f"Daily API limit reached ({self.calls_per_day} calls). Stop and resume tomorrow.")
                return False

            # Per-minute limit: wait until oldest call falls outside the window
            now = time.time()
            oldest = self._call_timestamps[0]
            wait_seconds = 60.0 - (now - oldest) + 0.5  # 0.5s safety buffer
            logger.info(f"Rate limit: waiting {wait_seconds:.1f}s before next API call")
            time.sleep(wait_seconds)

    def record_call(self):
        """Record that an API call was just made."""
        self._call_timestamps.append(time.time())
        today_str = date.today().isoformat()
        self.db.increment_daily_api_calls(today_str)

    def get_daily_usage(self):
        """Get today's API call count."""
        today_str = date.today().isoformat()
        return self.db.get_daily_api_calls(today_str)

    def get_remaining_daily(self):
        """Get remaining API calls for today."""
        return self.calls_per_day - self.get_daily_usage()
