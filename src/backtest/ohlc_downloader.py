import logging
import time
from datetime import datetime, timedelta, date

logger = logging.getLogger("ICICI_ORB_Bot")


class OHLCDownloader:
    """
    Downloads historical 1-minute OHLC data from Breeze API
    with rate limiting, resume capability, and progress tracking.

    Designed to be run daily until all stocks are downloaded.
    Respects API limits and saves progress after each chunk.
    """

    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 5  # seconds

    def __init__(self, api, db, rate_limiter, config):
        """
        Args:
            api: ICICIDirectAPI instance (already authenticated)
            db: BacktestDatabase instance
            rate_limiter: RateLimiter instance
            config: dict with full config (including nifty_50_stocks and backtest sections)
        """
        self.api = api
        self.db = db
        self.rate_limiter = rate_limiter
        self.config = config

        # Chunk size in days - 2 days of 1-min data = ~750 candles (under 1000 API limit)
        self.chunk_days = config.get("backtest", {}).get("chunk_days", 2)
        self.interval = config.get("backtest", {}).get("interval", "1minute")
        self.exchange_code = config.get("exchange_code", "NSE")

    def initialize_all_stocks(self):
        """
        Set up download_progress entries for all configured stocks.
        Idempotent -- safe to call multiple times.
        """
        stocks = self.config.get("nifty_50_stocks", [])
        start_date = self.config["backtest"]["start_date"]
        end_date = self.config["backtest"]["end_date"]

        for stock_code in stocks:
            self.db.init_stock_progress(stock_code, start_date, end_date)

        logger.info(f"Initialized progress tracking for {len(stocks)} stocks")

    def run(self):
        """
        Main download loop. Iterates through all stocks, downloading
        data in 2-day chunks. Respects rate limits and resumes from
        where it left off.

        Returns:
            dict with session summary (status, records_today, calls_today, etc.)
        """
        self.initialize_all_stocks()

        all_progress = self.db.get_all_progress()
        stocks_to_process = [
            p for p in all_progress if p['status'] != 'completed'
        ]

        if not stocks_to_process:
            logger.info("All stocks already downloaded!")
            return {
                "status": "complete",
                "stocks_completed_today": 0,
                "stocks_remaining": 0,
                "records_today": 0,
                "calls_today": 0
            }

        logger.info(f"Stocks remaining to download: {len(stocks_to_process)}")
        if self.rate_limiter:
            remaining = self.rate_limiter.get_remaining_daily()
            logger.info(f"API calls remaining today: {remaining}")

        total_records = 0
        total_calls = 0
        stocks_completed = 0

        for progress in stocks_to_process:
            stock_code = progress['stock_code']
            logger.info(f"=== Processing {stock_code} ===")

            result = self._download_stock(stock_code, progress)

            total_records += result['records']
            total_calls += result['calls']

            if result['status'] == 'completed':
                stocks_completed += 1
                logger.info(f"  {stock_code}: COMPLETED ({result['records']} records)")
            elif result['status'] == 'daily_limit':
                logger.warning(
                    f"Daily API limit reached during {stock_code}. "
                    f"Session summary: {total_records} records, {total_calls} calls. "
                    f"Resume tomorrow."
                )
                return {
                    "status": "daily_limit",
                    "stocks_completed_today": stocks_completed,
                    "stocks_remaining": len(stocks_to_process) - stocks_completed,
                    "records_today": total_records,
                    "calls_today": total_calls
                }
            elif result['status'] == 'error':
                logger.error(f"  {stock_code}: Error encountered, moving to next stock")
                continue

        return {
            "status": "complete" if stocks_completed == len(stocks_to_process) else "partial",
            "stocks_completed_today": stocks_completed,
            "stocks_remaining": len(stocks_to_process) - stocks_completed,
            "records_today": total_records,
            "calls_today": total_calls
        }

    def _download_stock(self, stock_code, progress):
        """
        Download all remaining chunks for a single stock.

        Returns:
            dict with status, records count, and calls count.
        """
        # Determine start date: resume from last_downloaded_date + 1 day
        if progress['last_downloaded_date']:
            chunk_start = datetime.strptime(
                progress['last_downloaded_date'], "%Y-%m-%d"
            ).date() + timedelta(days=1)
        else:
            chunk_start = datetime.strptime(
                progress['first_target_date'], "%Y-%m-%d"
            ).date()

        end_date = datetime.strptime(
            progress['last_target_date'], "%Y-%m-%d"
        ).date()

        records_total = 0
        calls_total = 0

        # Mark as in_progress
        self.db.update_stock_progress(
            stock_code, progress.get('last_downloaded_date'),
            'in_progress'
        )

        while chunk_start <= end_date:
            # Skip weekends to save API calls
            if chunk_start.weekday() >= 5:  # Saturday=5, Sunday=6
                chunk_start += timedelta(days=1)
                continue

            chunk_end = min(
                chunk_start + timedelta(days=self.chunk_days - 1),
                end_date
            )

            # Check rate limit -- blocks if per-minute limit, returns False if daily limit
            if self.rate_limiter and not self.rate_limiter.wait_if_needed():
                # Daily limit hit -- save progress and exit
                self.db.update_stock_progress(
                    stock_code,
                    (chunk_start - timedelta(days=1)).isoformat(),
                    'in_progress', records_total, calls_total
                )
                return {
                    'status': 'daily_limit',
                    'records': records_total,
                    'calls': calls_total
                }

            # Fetch this chunk with retry logic
            result = self._fetch_chunk_with_retry(stock_code, chunk_start, chunk_end)

            calls_total += 1
            if self.rate_limiter:
                self.rate_limiter.record_call()

            if result['success']:
                records = result['data']
                if records:
                    inserted = self.db.insert_ohlc_batch(records)
                    records_total += inserted
                    logger.info(
                        f"  {stock_code}: {chunk_start} to {chunk_end} "
                        f"-> {inserted} records inserted ({len(records)} fetched)"
                    )
                else:
                    logger.debug(
                        f"  {stock_code}: {chunk_start} to {chunk_end} "
                        f"-> no data (holiday/no trading)"
                    )

                # Save progress after each successful chunk
                self.db.update_stock_progress(
                    stock_code, chunk_end.isoformat(),
                    'in_progress', len(records) if records else 0, 1
                )
            else:
                error_msg = result['error']
                logger.error(
                    f"  {stock_code}: {chunk_start} to {chunk_end} "
                    f"-> FAILED: {error_msg}"
                )
                self.db.update_stock_progress(
                    stock_code,
                    (chunk_start - timedelta(days=1)).isoformat() if chunk_start > datetime.strptime(progress['first_target_date'], "%Y-%m-%d").date() else None,
                    'error', 0, 1, error=error_msg
                )
                return {
                    'status': 'error',
                    'records': records_total,
                    'calls': calls_total
                }

            # Move to next chunk
            chunk_start = chunk_end + timedelta(days=1)

        # All chunks for this stock are done
        self.db.update_stock_progress(
            stock_code, end_date.isoformat(), 'completed', 0, 0
        )
        return {
            'status': 'completed',
            'records': records_total,
            'calls': calls_total
        }

    def _fetch_chunk_with_retry(self, stock_code, chunk_start, chunk_end):
        """
        Fetch a single time chunk from the API with exponential backoff retry.

        Args:
            stock_code: Stock symbol
            chunk_start: date object for chunk start
            chunk_end: date object for chunk end

        Returns:
            dict: {'success': bool, 'data': list of tuples or None, 'error': str or None}
        """
        # Format dates for Breeze API (ISO8601 with market hours)
        from_date = f"{chunk_start.isoformat()}T09:15:00.000Z"
        to_date = f"{chunk_end.isoformat()}T15:30:00.000Z"

        params = {
            "interval": self.interval,
            "from_date": from_date,
            "to_date": to_date,
            "stock_code": stock_code,
            "exchange_code": self.exchange_code,
            "product_type": "cash"
        }

        max_retries = self.config.get("backtest", {}).get("max_retries", self.MAX_RETRIES)
        retry_delay = self.config.get("backtest", {}).get("retry_base_delay", self.RETRY_BASE_DELAY)

        for attempt in range(max_retries):
            try:
                response = self.api.get_historical_data_v2(params)

                if response.get('Status') == 200 and response.get('Success') is not None:
                    raw_data = response['Success']

                    if not raw_data:
                        return {'success': True, 'data': [], 'error': None}

                    # Transform API response to tuples for batch insert
                    records = []
                    for row in raw_data:
                        # Handle both 'datetime' and 'time' field names
                        dt = row.get('datetime') or row.get('time', '')
                        records.append((
                            stock_code,
                            str(dt),
                            float(row.get('open', 0)),
                            float(row.get('high', 0)),
                            float(row.get('low', 0)),
                            float(row.get('close', 0)),
                            int(float(row.get('volume', 0)))
                        ))

                    return {'success': True, 'data': records, 'error': None}

                elif response.get('Success') is None and response.get('Error'):
                    error_msg = str(response['Error'])

                    # Non-retryable errors
                    if any(term in error_msg.lower() for term in
                           ['invalid', 'not found', 'no data']):
                        # "No data" is not really an error - just no trading data for that period
                        if 'no data' in error_msg.lower():
                            return {'success': True, 'data': [], 'error': None}
                        return {'success': False, 'data': None, 'error': error_msg}

                    logger.warning(
                        f"  API error (attempt {attempt + 1}/{max_retries}): {error_msg}"
                    )
                else:
                    # No Success and no Error -- treat as empty data
                    return {'success': True, 'data': [], 'error': None}

            except Exception as e:
                logger.warning(
                    f"  Exception (attempt {attempt + 1}/{max_retries}): {e}"
                )

            # Exponential backoff before retry
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
                logger.info(f"  Retrying in {delay}s...")
                time.sleep(delay)

        return {
            'success': False,
            'data': None,
            'error': f"Failed after {max_retries} retries"
        }

    def get_download_summary(self):
        """
        Return a summary of download progress across all stocks.

        Returns:
            dict with counts by status, total records, and detail lists.
        """
        all_progress = self.db.get_all_progress()

        completed = [p for p in all_progress if p['status'] == 'completed']
        in_progress = [p for p in all_progress if p['status'] == 'in_progress']
        pending = [p for p in all_progress if p['status'] == 'pending']
        errored = [p for p in all_progress if p['status'] == 'error']

        total_records = sum(p['total_records'] for p in all_progress)
        total_calls = sum(p['total_api_calls'] for p in all_progress)

        return {
            'completed': len(completed),
            'in_progress': len(in_progress),
            'pending': len(pending),
            'errored': len(errored),
            'total_stocks': len(all_progress),
            'total_records': total_records,
            'total_api_calls': total_calls,
            'completed_stocks': [p['stock_code'] for p in completed],
            'in_progress_stocks': [p['stock_code'] for p in in_progress],
            'errored_stocks': [(p['stock_code'], p['last_error']) for p in errored]
        }
