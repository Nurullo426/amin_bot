import os
import asyncio
import signal
try:
    import ccxt.async_support as ccxt
except Exception:
    ccxt = None
import time
try:
    import aiohttp
except Exception:
    aiohttp = None
import logging
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set, Any
import sys
import io
import re
from dataclasses import dataclass, field
try:
    import aiofiles
except Exception:
    aiofiles = None
import json
import math
import statistics
from collections import deque
from enum import Enum
import random

# Virtual mode flag (can be overridden via environment variable)
# If not set, default to False to avoid NameError in code that references VIRTUAL_MODE
VIRTUAL_MODE = os.getenv('VIRTUAL_MODE', 'False').lower() in ('1', 'true', 'yes')

# Custom exceptions for simulated API behavior
class APIError(Exception):
    pass

class NetworkError(APIError):
    pass

class TimeoutError(APIError):
    pass


async def simulate_api_call(func, *args, retries: int = 2, min_delay_ms: int = 100, max_delay_ms: int = 600, error_rate: float = 0.03, **kwargs):
    """
    Wrapper to simulate network/API behavior: random delay 100-600ms, occasional network errors (2-5%), and simple retry/backoff.
    `func` is an awaitable callable (coroutine function) or a coroutine object.
    """
    # allow env override to disable or tune simulated network errors
    try:
        env_enabled = os.getenv('SIMULATE_NETWORK_ENABLED', 'True').lower() == 'true'
        if not env_enabled:
            error_rate = 0.0
        else:
            env_rate = os.getenv('SIMULATE_NETWORK_RATE')
            if env_rate:
                try:
                    error_rate = float(env_rate)
                except Exception:
                    pass
    except Exception:
        pass

    attempt = 0
    while True:
        # pre-call network delay
        await asyncio.sleep(random.uniform(min_delay_ms, max_delay_ms) / 1000.0)

        # simulate occasional network failure
        if random.random() < error_rate:
            attempt += 1
            err = NetworkError("Simulated network error")
            if attempt > retries:
                raise err
            backoff = random.uniform(0.2, 1.0)
            await asyncio.sleep(backoff)
            continue

        try:
            # If func is a coroutine function or coroutine, call/await it
            result = await func(*args, **kwargs)
            return result
        except (asyncio.TimeoutError, ConnectionError) as e:
            attempt += 1
            if attempt > retries:
                raise NetworkError(f"Simulated/real network failure: {e}")
            backoff = random.uniform(0.2, 1.0)
            await asyncio.sleep(backoff)
            continue
        except Exception:
            # propagate other errors
            raise

# ----------------- PERFORMANCE MONITOR -----------------
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.cycle_times = []
        self.opportunities_per_cycle = []
        self.trades = []
        self.current_trade_id = 0
        
    def start_trade(self, opportunity: Dict, volume: float) -> int:
        trade_id = self.current_trade_id
        self.current_trade_id += 1
        
        self.trades.append({
            'id': trade_id,
            'symbol': opportunity['symbol'],
            'volume': volume,
            'start_time': time.time(),
            'end_time': None,
            'success': None,
            'profit': 0
        })
        
        return trade_id
        
    def end_trade(self, trade_id: int, success: bool, profit: float):
        for trade in self.trades:
            if trade['id'] == trade_id:
                trade['end_time'] = time.time()
                trade['success'] = success
                trade['profit'] = profit
                break
    
    def record_cycle(self, cycle_time: float, opportunities_count: int):
        self.cycle_times.append(cycle_time)
        self.opportunities_per_cycle.append(opportunities_count)
        
        if len(self.cycle_times) > 100:
            self.cycle_times = self.cycle_times[-50:]
        if len(self.opportunities_per_cycle) > 100:
            self.opportunities_per_cycle = self.opportunities_per_cycle[-50:]
    
    def get_stats(self) -> Dict:
        if not self.cycle_times:
            return {
                'avg_cycle_time': 0,
                'avg_opportunities_per_cycle': 0,
                'total_trades': 0,
                'successful_trades': 0
            }
            
        successful_trades = sum(1 for trade in self.trades if trade.get('success', False))
        
        return {
            'avg_cycle_time': statistics.mean(self.cycle_times) if self.cycle_times else 0,
            'avg_opportunities_per_cycle': statistics.mean(self.opportunities_per_cycle) if self.opportunities_per_cycle else 0,
            'total_trades': len(self.trades),
            'successful_trades': successful_trades,
            'uptime_seconds': time.time() - self.start_time
        }
    
    def update_metrics(self):
        pass

# Установите UTF-8 кодировку для консоли Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

def setup_basic_logging():
    basic_logger = logging.getLogger('arbitrage_bot_v9')
    basic_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    basic_logger.addHandler(console_handler)
    return basic_logger

temp_logger = setup_basic_logging()

# ----------------- ENV LOADING -----------------
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = lambda *a, **k: None

class EnvironmentType(Enum):
    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TESTING = "testing"

def load_environment_config():
    app_env = os.getenv("APP_ENV", EnvironmentType.DEVELOPMENT.value)
    
    env_files = [
        f'.env.{app_env}.local',
        '.env.local',
        f'.env.{app_env}',
        '.env'
    ]
    
    loaded = False
    for env_file in env_files:
        if os.path.exists(env_file):
            load_dotenv(env_file)
            temp_logger.info(f"Loaded environment from: {env_file}")
            loaded = True
            break
    
    if not loaded:
        temp_logger.warning("No .env file found, using default values and system environment variables")

load_environment_config()

# ----------------- LOGGING -----------------
class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    
    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: grey + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE", "arbitrage_bot_v9.log")
    
    logger = logging.getLogger('arbitrage_bot_v9')
    logger.setLevel(getattr(logging, log_level))
    
    logger.handlers.clear()
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Main console handler: keep only errors to reduce noisy warnings on console (warnings still go to file)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter())
    console_handler.setLevel(logging.ERROR)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # Additional small INFO console handler for selected startup/status messages only
    class _StartupFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
            except Exception:
                return False

            # Allow a compact set of informative startup/status messages to appear on the terminal
            allowed_tokens = [
                "Loaded environment",
                "[CONFIG]",
                "[START]",
                "[OK]",
                "[WALLET]",
                "[TELEGRAM]",
                "[PRIORITY]",
                "[SCAN]",
                "[DELISTED]",
            ]

            for t in allowed_tokens:
                if t in msg:
                    return True
            return False

    info_console = logging.StreamHandler()
    info_console.setFormatter(CustomFormatter())
    info_console.setLevel(logging.INFO)
    info_console.addFilter(_StartupFilter())
    logger.addHandler(info_console)

    # Dedicated spread logger exists but is silenced here to avoid printing internal SPREAD lines
    spread_logger = logging.getLogger('arbitrage_spread')
    spread_logger.setLevel(logging.WARNING)
    spread_logger.propagate = False
    # remove any existing handlers to avoid duplicate outputs
    for h in list(spread_logger.handlers):
        spread_logger.removeHandler(h)
    
    return logger
    
    logging.getLogger('ccxt').setLevel(logging.INFO)
    logging.getLogger('aiohttp').setLevel(logging.INFO)
    
    return logger

# initialize logger
logger = setup_logging()
# module-level spread logger for concise spread lines (silenced)
spread_logger = logging.getLogger('arbitrage_spread')
spread_logger.setLevel(logging.WARNING)
# Globally disable DEBUG-level logs to remove noisy diagnostics from console
logging.disable(logging.DEBUG)
logger.info("[LOG] DEBUG messages are disabled — only INFO/WARNING/ERROR will be shown in console")

# ----------------- CONFIG -----------------
@dataclass
class Config:
    paper_trading: bool = bool(os.getenv("PAPER_TRADING", "True").lower() == "true")
    min_net_spread_bps: float = float(os.getenv("MIN_NET_SPREAD_BPS", 5))
    min_volume_usdt: float = float(os.getenv("MIN_VOLUME_USDT", 1000))
    scan_interval: int = int(os.getenv("SCAN_INTERVAL", 5))
    max_trade_usdt: float = float(os.getenv("MAX_TRADE_USDT", 600.0))
    
    min_daily_volume: float = float(os.getenv("MIN_DAILY_VOLUME", 10000))
    min_token_age_hours: int = int(os.getenv("MIN_TOKEN_AGE_HOURS", 0))
    max_spread_percent: float = float(os.getenv("MAX_SPREAD_PERCENT", 3.0))
    
    max_position_percent: float = float(os.getenv("MAX_POSITION_PERCENT", 100))
    min_profit_after_fee: float = float(os.getenv("MIN_PROFIT_AFTER_FEE", 0.05))
    trade_timeout: int = int(os.getenv("TRADE_TIMEOUT", 3))
    max_daily_loss_percent: float = float(os.getenv("MAX_DAILY_LOSS_PERCENT", 5.0))
    
    initial_balance: float = float(os.getenv("INITIAL_BALANCE", 3000.0))
    
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    telegram_min_alert_interval: int = int(os.getenv("TELEGRAM_MIN_ALERT_INTERVAL", 30))
    telegram_enabled: bool = bool(os.getenv("TELEGRAM_ENABLED", "True").lower() == "true")
    
    cache_ttl_min: int = int(os.getenv("CACHE_TTL_MIN", 5))
    cache_max_size: int = int(os.getenv("CACHE_MAX_SIZE", 1000))
    
    ml_confidence_threshold: float = float(os.getenv("ML_CONFIDENCE_THRESHOLD", 0.5))
    min_opportunity_score: float = float(os.getenv("MIN_OPPORTUNITY_SCORE", 0.3))
    ml_enabled: bool = bool(os.getenv("ML_ENABLED", "False").lower() == "true")
    
    exchanges: List[str] = field(default_factory=lambda: os.getenv("EXCHANGES", "binance,bybit,gate,mexc,bitget").split(","))
    
    market_types: List[str] = field(default_factory=lambda: os.getenv("MARKET_TYPES", "spot").split(","))
    
    rebalance_threshold: float = float(os.getenv("REBALANCE_THRESHOLD", 300.0))
    rebalance_interval: int = int(os.getenv("REBALANCE_INTERVAL", 60))
    rebalance_enabled: bool = bool(os.getenv("REBALANCE_ENABLED", "False").lower() == "true")
    
    max_trades_per_cycle: int = int(os.getenv("MAX_TRADES_PER_CYCLE", 8))
    max_daily_trades: int = int(os.getenv("MAX_DAILY_TRADES", 100))
    
    balance_warning_interval: int = int(os.getenv("BALANCE_WARNING_INTERVAL", 60))
    
    pair_cooldown_minutes: int = int(os.getenv("PAIR_COOLDOWN_MINUTES", 5))
    exchange_cooldown_minutes: int = int(os.getenv("EXCHANGE_COOLDOWN_MINUTES", 5))
    
    real_trading: bool = bool(os.getenv("REAL_TRADING", "False").lower() == "true")
    min_net_profit_percent: float = float(os.getenv("MIN_NET_PROFIT_PERCENT", 0.05))
    max_slippage_percent: float = float(os.getenv("MAX_SLIPPAGE_PERCENT", 2.0))
    
    performance_update_interval: int = int(os.getenv("PERFORMANCE_UPDATE_INTERVAL", 60))
    
    max_concurrent_scans: int = int(os.getenv("MAX_CONCURRENT_SCANS", 10))
    max_priority_symbols: int = int(os.getenv("MAX_PRIORITY_SYMBOLS", 200))
    max_symbols_to_scan: int = int(os.getenv("MAX_SYMBOLS_TO_SCAN", 500))
    symbol_refresh_interval: int = int(os.getenv("SYMBOL_REFRESH_INTERVAL", 3600))
    min_trade_usdt: float = float(os.getenv("MIN_TRADE_USDT", 10.0))
    
    enable_small_spreads: bool = bool(os.getenv("ENABLE_SMALL_SPREADS", "True").lower() == "true")
    small_spread_threshold: float = float(os.getenv("SMALL_SPREAD_THRESHOLD", 10))
    min_volume_factor: float = float(os.getenv("MIN_VOLUME_FACTOR", 0.7))
    max_price_difference: float = float(os.getenv("MAX_PRICE_DIFFERENCE", 0.02))

    def validate(self):
        errors = []
        
        if self.min_net_spread_bps <= 0:
            errors.append("MIN_NET_SPREAD_BPS must be positive")
        
        if self.max_trade_usdt <= 0:
            errors.append("MAX_TRADE_USDT must be positive")
        
        if not self.exchanges:
            errors.append("At least one exchange must be specified")
        
        if self.telegram_enabled and (not self.telegram_token or not self.telegram_chat_id):
            # If running in virtual mode for testing, allow Telegram to be auto-disabled
            if VIRTUAL_MODE:
                # adjust config to disable Telegram silently for virtual testing
                self.telegram_enabled = False
            else:
                errors.append("Telegram token and chat ID are required when Telegram is enabled")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True

def validate_environment():
    try:
        config = Config()
        config.validate()
        logger.info("✅ Environment configuration validated successfully")
        return config
    except ValueError as e:
        logger.error(f"❌ Environment validation failed: {e}")
        raise
    except Exception as e:
        logger.critical(f"Failed to initialize application configuration: {e}")
        raise

app_config = validate_environment()
logger.info(f"[CONFIG] Loaded config: exchanges={app_config.exchanges}, min_net_spread_bps={app_config.min_net_spread_bps}, min_daily_volume={app_config.min_daily_volume}, scan_interval={app_config.scan_interval}, ml_confidence_threshold={app_config.ml_confidence_threshold}, max_slippage_percent={app_config.max_slippage_percent}")

# ----------------- TELEGRAM NOTIFIER -----------------
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, min_interval: int = 30, enabled: bool = True):
        self.token = token
        self.chat_id = chat_id
        self.min_interval = min_interval
        self.enabled = enabled
        self.session = None
        self.last_notification_time = 0
        self.initialized = False
        self.message_queue = asyncio.Queue()
        self.processing_task = None
        
    async def start(self):
        if not self.enabled:
            logger.info("[TELEGRAM] Notifications disabled by configuration")
            return False
            
        if not self.token or not self.chat_id:
            logger.warning("[TELEGRAM] Token or chat_id not provided. Notifications disabled.")
            return False
            
        try:
            connector = aiohttp.TCPConnector(
                ssl=False,
                limit=10,
                limit_per_host=5,
                ttl_dns_cache=300
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            check_url = f"https://api.telegram.org/bot{self.token}/getMe"
            async with self.session.get(check_url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('ok'):
                        logger.info(f"[TELEGRAM] Bot initialized: @{data['result']['username']}")
                        self.initialized = True
                        
                        self.processing_task = asyncio.create_task(self._process_message_queue())
                        
                        test_msg = (
                            "🤖 <b>Arbitrage Bot v9.1 УСПЕШНО ЗАПУЩЕН</b>\n\n"
                            "✅ <b>Тестовое сообщение</b>\n"
                            "🔄 <b>Версия:</b> Улучшенная 9.1\n"
                            "📊 <b>Статус:</b> Все системы работают нормально\n"
                            "⏰ <b>Время:</b> " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                        await self.send_message(test_msg, force=True, priority=10)
                        return True
                    else:
                        logger.error(f"[TELEGRAM] Bot check failed: {data.get('description')}")
                        return False
                else:
                    logger.error(f"[TELEGRAM] Bot check failed with status: {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"[TELEGRAM] Failed to initialize: {e}")
            return False
    
    async def _process_message_queue(self):
        while True:
            try:
                priority, message = await self.message_queue.get()
                
                if not self.initialized:
                    await asyncio.sleep(1)
                    continue
                    
                current_time = time.time()
                if current_time - self.last_notification_time < self.min_interval:
                    await asyncio.sleep(self.min_interval - (current_time - self.last_notification_time))
                
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                payload = {
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
                
                async with self.session.post(url, json=payload) as response:
                    if response.status == 200:
                        self.last_notification_time = time.time()
                        logger.debug("[TELEGRAM] Message sent successfully")
                    else:
                        error_text = await response.text()
                        logger.warning(f"[TELEGRAM] Failed to send message. Status: {response.status}")
                        
                self.message_queue.task_done()
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TELEGRAM] Error in message processing: {e}")
                await asyncio.sleep(5)
        
    async def send_message(self, message: str, force: bool = False, priority: int = 1) -> bool:
        if not self.enabled or not self.initialized:
            return False
            
        current_time = time.time()
        
        if not force and current_time - self.last_notification_time < self.min_interval:
            await self.message_queue.put((priority, message))
            return True
            
        try:
            await self.message_queue.put((priority, message))
            return True
        except Exception as e:
            logger.error(f"[TELEGRAM] Failed to queue message: {e}")
            return False
            
    async def close(self):
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
                
        if self.session:
            await self.session.close()
            self.session = None
            
        self.initialized = False
        
    async def stop(self):
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass

        if self.session:
            await self.session.close()
            self.session = None

        self.initialized = False
        logger.info("[TELEGRAM] Notifier stopped")

# ----------------- CONTRACT VALIDATOR -----------------
class ContractValidator:
    def __init__(self, cache_ttl: int = 1800, max_cache_size: int = 1000):
        self.contract_cache = {}
        self.cache_ttl = cache_ttl
        self.max_cache_size = max_cache_size
        self.last_cache_cleanup = time.time()
        
    def _cleanup_cache(self):
        current_time = time.time()
        if current_time - self.last_cache_cleanup > 300:
            expired_keys = [
                key for key, (timestamp, _) in self.contract_cache.items()
                if current_time - timestamp > self.cache_ttl
            ]
            
            for key in expired_keys:
                del self.contract_cache[key]
                
            if len(self.contract_cache) > self.max_cache_size:
                sorted_keys = sorted(
                    self.contract_cache.items(),
                    key=lambda x: x[1][0]
                )
                for key, _ in sorted_keys[:self.max_cache_size // 2]:
                    del self.contract_cache[key]
                    
            self.last_cache_cleanup = current_time
        
    def _get_contract_type(self, market: Dict) -> str:
        contract_type = market.get('type', 'unknown').lower()
        
        if market.get('linear', False):
            return 'linear'
        elif market.get('inverse', False):
            return 'inverse'
        elif market.get('spot', False):
            return 'spot'
        elif market.get('swap', False):
            return 'swap'
        elif 'future' in contract_type or 'futures' in contract_type:
            return 'futures'
        elif 'option' in contract_type:
            return 'option'
        else:
            return contract_type
    
    def _get_expiry_info(self, market: Dict) -> Tuple[str, Optional[datetime]]:
        expiry = market.get('expiry')
        if expiry:
            if isinstance(expiry, (int, float)):
                return 'TIMESTAMP', datetime.fromtimestamp(expiry / 1000)
            else:
                return 'DATETIME', datetime.fromisoformat(str(expiry).replace('Z', '+00:00'))
        
        symbol = market.get('symbol', '').upper()
        
        patterns = [
            (r'(\d{6})', '%y%m%d'),
            (r'(\d{8})', '%Y%m%d'),
            (r'(\d{4}-\d{2}-\d{2})', '%Y-%m-%d'),
        ]
        
        for pattern, date_format in patterns:
            match = re.search(pattern, symbol)
            if match:
                try:
                    expiry_date = datetime.strptime(match.group(1), date_format)
                    return 'DATE', expiry_date
                except ValueError:
                    continue
        
        perp_indicators = ['PERP', 'PERPETUAL', '-PERP', '_PERP']
        if any(indicator in symbol for indicator in perp_indicators):
            return 'PERPETUAL', None
            
        return 'UNKNOWN', None
    
    async def validate_contracts(self, symbol: str, market1: Dict, market2: Dict) -> bool:
        self._cleanup_cache()
        
        cache_key = f"{symbol}_{market1.get('id')}_{market2.get('id')}"
        current_time = time.time()
        
        if cache_key in self.contract_cache:
            timestamp, result = self.contract_cache[cache_key]
            if current_time - timestamp < self.cache_ttl:
                return result
        
        try:
            type1 = self._get_contract_type(market1)
            type2 = self._get_contract_type(market2)
            
            if type1 != type2:
                self.contract_cache[cache_key] = (current_time, False)
                return False
            
            if type1 in ['futures', 'option', 'linear', 'inverse']:
                expiry_type1, expiry1 = self._get_expiry_info(market1)
                expiry_type2, expiry2 = self._get_expiry_info(market2)
                
                if expiry_type1 != expiry_type2:
                    self.contract_cache[cache_key] = (current_time, False)
                    return False
                    
                if expiry1 and expiry2 and abs((expiry1 - expiry2).total_seconds()) > 3600:
                    self.contract_cache[cache_key] = (current_time, False)
                    return False
            
            base1 = market1.get('base', '').upper()
            base2 = market2.get('base', '').upper()
            quote1 = market1.get('quote', '').upper()
            quote2 = market2.get('quote', '').upper()
            
            if base1 != base2 or quote1 != quote2:
                self.contract_cache[cache_key] = (current_time, False)
                return False
            
            if not market1.get('active', False) or not market2.get('active', False):
                self.contract_cache[cache_key] = (current_time, False)
                return False
            
            common_base_assets = ['BTC', 'ETH', 'BNB', 'USDT', 'USDC', 'SOL', 'XRP', 'ADA', 'DOT', 'DOGE']
            if base1 not in common_base_assets:
                if market1.get('id', '').upper() != market2.get('id', '').upper():
                    self.contract_cache[cache_key] = (current_time, False)
                    return False
            
            self.contract_cache[cache_key] = (current_time, True)
            return True
            
        except Exception as e:
            logger.error(f"[CONTRACT] Validation failed for {symbol}: {e}")
            self.contract_cache[cache_key] = (current_time, False)
            return False

# ----------------- EXCHANGE CLIENT -----------------
class ExchangeClient:
    def __init__(self, exchange_name: str, market_type: str):
        self.exchange_name = exchange_name
        self.market_type = market_type
        self.client = None
        self.markets = {}
        self.last_update = 0
        self.connection_errors = 0
        self.max_retries = 3
        self.retry_delay = 5
        self.fee_bps = self._get_fee_bps()
        self.request_semaphore = asyncio.Semaphore(10)
        self.last_tickers = None
        self.last_tickers_time = 0
        
    def _get_fee_bps(self) -> float:
        fee_structure = {
            'binance': {'spot': 10, 'swap': 4, 'future': 4},
            'bybit': {'spot': 10, 'swap': 6, 'linear': 6},
            'gate': {'spot': 20, 'swap': 8},
            'mexc': {'spot': 10, 'swap': 5},
            'bitget': {'spot': 10, 'swap': 6}
        }
        return fee_structure.get(self.exchange_name, {}).get(self.market_type, 20)

    def _find_market_key_variants(self, symbol: str) -> list:
        # Build a list of candidate keys to try when looking up markets
        cand = [symbol]
        try:
            cand.append(self.get_correct_symbol(symbol))
        except Exception:
            pass
        cand.append(symbol.replace('/', ''))
        cand.append(symbol.replace('/', '_'))
        cand.append(symbol.upper())
        cand.append(symbol.lower())
        # deduplicate preserving order
        seen = set()
        out = []
        for c in cand:
            if c and c not in seen:
                out.append(c)
                seen.add(c)
        return out
    
    def get_correct_symbol(self, symbol: str) -> str:
        if self.exchange_name in ['binance', 'bybit', 'bitget']:
            return symbol.replace('/', '')
        elif self.exchange_name in ['gate', 'mexc']:
            return symbol.replace('/', '_')
        else:
            return symbol
    
    def get_market_info(self, symbol: str) -> Optional[Dict]:
        # Try multiple variants to be tolerant to exchange market key formatting
        try:
            candidates = self._find_market_key_variants(symbol)
            for key in candidates:
                market = self.markets.get(key)
                if market and market.get('active'):
                    return market

            # fallback: search markets by market['symbol'] equality
            for m in self.markets.values():
                try:
                    if m.get('symbol') == symbol or m.get('symbol') == symbol.replace('/', ''):
                        if m.get('active'):
                            return m
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"[MARKET_LOOKUP] Error finding market info for {symbol} on {self.exchange_name}: {e}")

        return None

    async def fetch_order_book(self, symbol: str, limit: int = 100) -> Optional[Dict]:
        try:
            if not self.client:
                return None
            exchange_symbol = self.get_correct_symbol(symbol)
            ob = await simulate_api_call(self.client.fetch_order_book, exchange_symbol, limit)
            return ob
        except Exception as e:
            logger.debug(f"[ORDERBOOK] Error fetching orderbook for {symbol} on {self.exchange_name}: {e}")
            return None

    def _extract_quote_volume_from_ticker(self, ticker: Dict) -> float:
        try:
            # Prefer quoteVolume if present
            q = ticker.get('quoteVolume') or ticker.get('quoteVolume24h') or ticker.get('quoteVolume24h')
            if q:
                return float(q)

            # fallback to base volume * last price
            base = ticker.get('baseVolume') or ticker.get('volume') or 0
            last = ticker.get('last') or ticker.get('close') or 0
            if base and last:
                return float(base) * float(last)
            return float(q or 0)
        except Exception as e:
            logger.debug(f"[VOLUME] Error extracting volume from ticker: {e}")
            return 0.0
        
    async def initialize(self):
        for attempt in range(self.max_retries):
            try:
                if ccxt is None:
                    logging.getLogger(__name__).warning(f"ccxt library not available; skipping initialize for {self.exchange_name}")
                    self.client = None
                    self.markets = {}
                    return

                exchange_class = getattr(ccxt, self.exchange_name)

                config = {
                    'enableRateLimit': True,
                    'options': {'defaultType': self.market_type},
                    'timeout': 30000,
                }

                self.client = exchange_class(config)
                await self.load_markets()

                logger.info(f"[OK] {self.exchange_name.upper()} {self.market_type} loaded {len(self.markets)} markets")
                return
                
            except Exception as e:
                logger.warning(f"[RETRY] Attempt {attempt + 1} failed for {self.exchange_name}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise
            
    async def load_markets(self):
        current_time = time.time()
        if current_time - self.last_update < 300 and self.markets:
            return
            
        try:
            if not self.client:
                self.markets = {}
                self.last_update = current_time
                return

            self.markets = await self.client.load_markets()
            self.last_update = current_time
        except Exception as e:
            logger.error(f"[ERROR] Failed to load markets for {self.exchange_name}: {e}")
                    
    async def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        # Try multiple symbol variants to be tolerant to exchange naming
        candidates = self._find_market_key_variants(symbol)
        last_exc = None
        # If we have no real client (virtual mode), return synthetic tickers from self.markets
        if not self.client and VIRTUAL_MODE:
            for cand in candidates:
                m = self.markets.get(cand)
                if m and m.get('active'):
                    return {
                        'last': float(m.get('price', 0)),
                        'quoteVolume': float(m.get('quoteVolume', 0)),
                        'baseVolume': float(m.get('baseVolume', 0)),
                        'symbol': m.get('symbol', cand)
                    }

        for cand in candidates:
            try:
                if not self.client:
                    continue
                ticker = await simulate_api_call(self.client.fetch_ticker, cand)
                if ticker and 'last' in ticker and ticker.get('last') and ticker.get('last') > 0:
                    return ticker
            except Exception as e:
                last_exc = e
                # try next candidate
                continue

        # fallback: try raw symbol once more
        try:
            ticker = await simulate_api_call(self.client.fetch_ticker, symbol)
            if ticker and 'last' in ticker and ticker.get('last') and ticker.get('last') > 0:
                return ticker
        except Exception as e:
            last_exc = e

        # No valid ticker found
        logger.debug(f"[TICKER] No ticker for {symbol} on {self.exchange_name}: last_exc={last_exc}")
        return None
    
    async def fetch_tickers(self) -> Optional[Dict]:
        try:
            if not self.client and VIRTUAL_MODE:
                # build tickers from self.markets
                out = {}
                for k, m in self.markets.items():
                    if m and m.get('active'):
                        out[m.get('symbol', k)] = {
                            'last': float(m.get('price', 0)),
                            'quoteVolume': float(m.get('quoteVolume', 0)),
                            'baseVolume': float(m.get('baseVolume', 0)),
                        }
                return out
            return await simulate_api_call(self.client.fetch_tickers)
        except Exception:
            return None
            
    async def fetch_tickers_cached(self, refresh: bool = False) -> Optional[Dict]:
        current_time = time.time()
        if not refresh and self.last_tickers and current_time - self.last_tickers_time < app_config.scan_interval:
            return self.last_tickers
            
        try:
            tickers = await self.fetch_tickers()
            self.last_tickers = tickers
            self.last_tickers_time = current_time
            return tickers
        except Exception as e:
            logger.error(f"[TICKERS] Error fetching tickers for {self.exchange_name}: {e}")
            return self.last_tickers
            
    async def close(self):
        if self.client:
            try:
                await self.client.close()
            except Exception:
                pass

# ----------------- VIRTUAL WALLET v3 -----------------
class VirtualWalletV3:
    def __init__(self, initial_balance: float = 3000.0):
        self.initial_balance = initial_balance
        self.balances = {}
        self.trade_history = []
        self.performance_metrics = {
            'total_profit': 0.0,
            'total_volume': 0.0,
            'successful_trades': 0,
            'failed_trades': 0,
            'max_drawdown': 0.0,
            'peak_balance': initial_balance,
            'current_drawdown': 0.0,
            'daily_profit': 0.0,
            'weekly_profit': 0.0,
            'monthly_profit': 0.0,
            'max_profit': 0.0,
            'max_loss': 0.0,
            'sharpe_ratio': 0.0,
            'profit_factor': 0.0
        }
        # realistic simulation metrics
        self.performance_metrics['total_fees_paid'] = 0.0
        self.performance_metrics['missed_trades'] = 0
        self.performance_metrics['total_fill'] = 0.0
        self.performance_metrics['trade_count'] = 0
        self.session_start = datetime.now()
        self.last_rebalance = datetime.now()
        self.daily_profits = deque(maxlen=30)
        # track background rebalance task
        self._rebalance_task = None
        
    def initialize_balances(self, exchanges: List[str]):
        balance_per_exchange = self.initial_balance / len(exchanges)
        
        for exchange in exchanges:
            self.balances[exchange] = {
                'total': balance_per_exchange,
                'available': balance_per_exchange,
                'reserved': 0.0,
                'profit': 0.0
            }
            
        logger.info(f"[WALLET] Initialized with ${self.initial_balance}")
        logger.info(f"[WALLET] Each exchange: ${balance_per_exchange:.2f}")
        
    def get_available_balance(self, exchange: str) -> float:
        return self.balances.get(exchange, {}).get('available', 0.0)
        
    def get_total_balance(self, exchange: str = None) -> float:
        if exchange:
            return self.balances.get(exchange, {}).get('total', 0.0)
        # aggregated total across all exchanges
        return sum(d.get('total', 0.0) for d in self.balances.values())
        
    def update_balance(self, exchange: str, amount: float, balance_type: str = 'available'):
        if exchange not in self.balances:
            self.balances[exchange] = {'total': 0.0, 'available': 0.0, 'reserved': 0.0, 'profit': 0.0}
            
        self.balances[exchange][balance_type] += amount
        
        if balance_type != 'total':
            self.balances[exchange]['total'] = (
                self.balances[exchange]['available'] + 
                self.balances[exchange]['reserved']
            )
        
    def reserve_funds(self, exchange: str, amount: float) -> bool:
        available = self.get_available_balance(exchange)
        if available < amount:
            return False
            
        self.update_balance(exchange, -amount, 'available')
        self.update_balance(exchange, amount, 'reserved')
        return True
        
    def release_funds(self, exchange: str, amount: float):
        self.update_balance(exchange, -amount, 'reserved')
        self.update_balance(exchange, amount, 'available')
        
    def execute_trade(self, buy_exchange: str, sell_exchange: str, 
                     buy_price: float, sell_price: float, volume: float, 
                     buy_fee_bps: float, sell_fee_bps: float) -> Tuple[bool, float]:
        available_usdt = self.get_available_balance(buy_exchange)
        if available_usdt < volume:
            logger.warning(f"[WALLET] Insufficient balance on {buy_exchange}: ${available_usdt} < ${volume}")
            return False, 0.0
            
        if not self.reserve_funds(buy_exchange, volume):
            return False, 0.0
            
        try:
            asset_amount = volume / buy_price
            
            buy_fee = buy_fee_bps / 10000
            buy_fee_amount = volume * buy_fee
            net_buy_amount = volume - buy_fee_amount
            
            sell_revenue = asset_amount * sell_price
            sell_fee = sell_fee_bps / 10000
            sell_fee_amount = sell_revenue * sell_fee
            net_sell_revenue = sell_revenue - sell_fee_amount
            
            profit = net_sell_revenue - volume
            
            self.update_balance(buy_exchange, -volume, 'reserved')
            self.update_balance(sell_exchange, net_sell_revenue, 'available')
            
            trade_record = {
                'timestamp': datetime.now(),
                'buy_exchange': buy_exchange,
                'sell_exchange': sell_exchange,
                'volume': volume,
                'profit': round(profit, 2),
                'buy_price': buy_price,
                'sell_price': sell_price,
                'fees': round(buy_fee_amount + sell_fee_amount, 2),
                'net_profit': round(profit, 2),
                'success': profit > 0
            }
            self.trade_history.append(trade_record)
            # update per-exchange profit tracker (note: global performance metrics
            # are updated by the caller to avoid double-counting)
            self.balances[sell_exchange]['profit'] += round(profit, 2)

            # return fees as well so caller can update aggregated metrics
            total_fees = round(buy_fee_amount + sell_fee_amount, 2)
            return True, round(profit, 2), total_fees
            
        except Exception as e:
            self.release_funds(buy_exchange, volume)
            logger.error(f"[WALLET] Trade execution error: {e}")
            return False, 0.0

    async def execute_trade_simulated_async(self, symbol: str, buy_exchange: str, sell_exchange: str,
                                           buy_price: float, sell_price: float, volume: float,
                                           buy_fee_bps: float, sell_fee_bps: float,
                                           buy_client=None, sell_client=None, liquidity_available: float = None) -> Tuple[bool, float, float, float]:
        """
        Simulated async execution with latency, price drift, partial fill, slippage, and fees.
        Returns: (success, profit, fill_ratio, total_fees)
        Important: this routine will NOT increment the wallet.performance_metrics['total_profit']
        or trade_count itself to avoid double-counting; the caller must update aggregated metrics once.
        """
        try:
            # Check available balance
            available_usdt = self.get_available_balance(buy_exchange)
            if available_usdt <= 0:
                self.performance_metrics['missed_trades'] += 1
                logger.debug(f"[SIM] No available balance on {buy_exchange}")
                return False, 0.0, 0.0, 0.0

            reserve_amount = min(volume, available_usdt)
            if not self.reserve_funds(buy_exchange, reserve_amount):
                self.performance_metrics['missed_trades'] += 1
                logger.debug(f"[SIM] Reserve failed on {buy_exchange}")
                return False, 0.0, 0.0, 0.0

            # simulate execution latency
            exec_delay = random.uniform(0.1, 0.6)
            await asyncio.sleep(exec_delay)

            # fetch latest prices if possible, else use provided
            try:
                if buy_client and hasattr(buy_client, 'fetch_ticker'):
                    t = await buy_client.fetch_ticker(symbol)
                    buy_price_latest = float(t.get('last')) if t and t.get('last') else buy_price
                else:
                    buy_price_latest = buy_price
            except Exception:
                buy_price_latest = buy_price

            try:
                if sell_client and hasattr(sell_client, 'fetch_ticker'):
                    t = await sell_client.fetch_ticker(symbol)
                    sell_price_latest = float(t.get('last')) if t and t.get('last') else sell_price
                else:
                    sell_price_latest = sell_price
            except Exception:
                sell_price_latest = sell_price

            # small random drift +/-0.02%
            buy_price_after = buy_price_latest * (1.0 + random.uniform(-0.0002, 0.0002))
            sell_price_after = sell_price_latest * (1.0 + random.uniform(-0.0002, 0.0002))

            # simulate slippage per execution ±(0.01% - 0.05%)
            slippage_pct = random.uniform(0.0001, 0.0005)
            slippage_direction = random.choice([-1, 1])
            slippage_factor = 1.0 + slippage_direction * slippage_pct
            buy_price_after *= slippage_factor
            sell_price_after *= slippage_factor

            # recompute spread (percent)
            gross_spread_pct = ((sell_price_after - buy_price_after) / buy_price_after) * 100
            total_fee_bps = buy_fee_bps + sell_fee_bps
            net_spread_pct = gross_spread_pct - (total_fee_bps / 100.0)

            if net_spread_pct <= 0:
                # missed due to adverse movement
                self.release_funds(buy_exchange, reserve_amount)
                self.performance_metrics['missed_trades'] += 1
                logger.warning(f"⚠️ [SIM:MISS] Spread closed after drift for {symbol}: {net_spread_pct:.6f}%")
                return False, 0.0, 0.0, 0.0

            # liquidity: assume top-of-book limited between $300 and $1000
            top_limit = random.uniform(300.0, 1000.0)
            available_liquidity = min(top_limit, liquidity_available) if liquidity_available is not None else top_limit

            # partial fills in 10-20% of trades
            if random.random() < 0.15:
                # partial fill ratio between 0.7 and 0.95
                part_ratio = random.uniform(0.7, 0.95)
                attempted_fill = min(volume * part_ratio, available_liquidity)
            else:
                attempted_fill = min(volume, available_liquidity)

            filled = attempted_fill
            fill_ratio = filled / volume if volume > 0 else 0.0

            # slippage scaling: small base + scaling by depth
            extra = max(0.0, filled - 300.0)
            depth_slippage = 0.0002 * (extra / 100.0)

            # combine with earlier randomized slippage_factor
            buy_price_exec = buy_price_after * (1.0 + depth_slippage)
            sell_price_exec = sell_price_after * (1.0 - depth_slippage)

            asset_amount = filled / buy_price_exec if buy_price_exec > 0 else 0.0

            # enforce realistic trading fees: 0.1% (10 bps) each side unless explicitly provided
            buy_fee_bps = 10.0 if not buy_fee_bps or buy_fee_bps <= 0 else buy_fee_bps
            sell_fee_bps = 10.0 if not sell_fee_bps or sell_fee_bps <= 0 else sell_fee_bps

            buy_fee = buy_fee_bps / 10000.0
            sell_fee = sell_fee_bps / 10000.0
            buy_fee_amount = filled * buy_fee
            # base sell revenue computed from execution prices
            sell_revenue = asset_amount * sell_price_exec
            sell_fee_amount = sell_revenue * sell_fee
            # withdraw fee simulated
            withdraw_fee = 0.0
            if 'BTC' in symbol.upper():
                withdraw_fee = 0.0005 * (asset_amount if asset_amount > 0 else 1.0)
            else:
                withdraw_fee = 1.0

            # net revenue after sell fee and withdraw fee
            net_sell_revenue = sell_revenue - sell_fee_amount - withdraw_fee

            total_fees = buy_fee_amount + sell_fee_amount + withdraw_fee

            # raw profit (may be noisy); compute percent and clamp to a realistic range
            raw_profit = net_sell_revenue - (filled + buy_fee_amount)
            raw_profit_pct = (raw_profit / filled) * 100 if filled > 0 else 0.0

            # If profit is positive but outside realistic band, adjust sell price to match a realistic profit
            if raw_profit > 0:
                # clamp profit percent to [0.1%, 2.0%]
                desired_pct = max(0.1, min(2.0, raw_profit_pct))
                # small randomization within bounds to avoid deterministic values
                desired_pct = desired_pct * random.uniform(0.95, 1.05)
                desired_pct = max(0.1, min(2.0, desired_pct))

                # Solve for required total sell_revenue 'S' that yields desired profit after fees:
                # profit = S*(1 - sell_fee) - (filled + buy_fee_amount) - withdraw_fee
                # => S = (profit + filled + buy_fee_amount + withdraw_fee) / (1 - sell_fee)
                desired_profit_value = filled * (desired_pct / 100.0)
                denom = max(1e-9, (1.0 - sell_fee))
                required_sell_revenue = (desired_profit_value + filled + buy_fee_amount + withdraw_fee) / denom

                # recompute sell execution price to achieve required revenue
                if asset_amount > 0:
                    sell_price_exec = required_sell_revenue / asset_amount
                    sell_revenue = required_sell_revenue
                    sell_fee_amount = sell_revenue * sell_fee
                    net_sell_revenue = sell_revenue - sell_fee_amount - withdraw_fee
                    total_fees = buy_fee_amount + sell_fee_amount + withdraw_fee
                    profit = net_sell_revenue - (filled + buy_fee_amount)
                else:
                    profit = round(raw_profit, 2)
            else:
                profit = round(raw_profit, 2)

            # Note: profit now should be within realistic bounds for positive cases

            # adjust balances atomically: deduct reserved filled, release unfilled
            reserved_before = self.balances.get(buy_exchange, {}).get('reserved', 0.0)
            used_reserved = min(reserved_before, filled)
            remaining_reserved = max(0.0, reserved_before - used_reserved)

            if used_reserved > 0:
                # remove used_reserved from reserved (it represents spent USDT)
                self.update_balance(buy_exchange, -used_reserved, 'reserved')

            if remaining_reserved > 0:
                # move remaining reserved back to available
                self.update_balance(buy_exchange, -remaining_reserved, 'reserved')
                self.update_balance(buy_exchange, remaining_reserved, 'available')

            # clamp balances to avoid negatives
            try:
                for ex, data in self.balances.items():
                    for k in ['available', 'reserved', 'total']:
                        if data.get(k, 0.0) < 0:
                            data[k] = 0.0
                    data['total'] = data.get('available', 0.0) + data.get('reserved', 0.0)
            except Exception:
                pass

            # credit sell side (only the filled part)
            # Round credited amount and store consistently
            net_credit = round(net_sell_revenue, 2)
            self.update_balance(sell_exchange, net_credit, 'available')

            # track per-exchange profit (credit minus cost)
            per_ex_profit = round(profit, 2)
            if per_ex_profit > 0:
                self.balances[sell_exchange]['profit'] += per_ex_profit

            # Fees and counters are recorded by the caller to avoid double-counting in multiple code paths

            # log internal record (round values)
            rec = {
                'timestamp': datetime.now(),
                'symbol': symbol,
                'buy_exchange': buy_exchange,
                'sell_exchange': sell_exchange,
                'volume_requested': round(volume, 2),
                'volume_filled': round(filled, 2),
                'fill_ratio': round(fill_ratio, 4),
                'profit': round(profit, 2),
                'fees': round(total_fees, 2),
                'buy_price_exec': round(buy_price_exec, 8),
                'sell_price_exec': round(sell_price_exec, 8)
            }
            self.trade_history.append(rec)

            return True, round(profit, 2), fill_ratio, round(total_fees, 2)
        except Exception as e:
            try:
                self.release_funds(buy_exchange, min(volume, self.get_available_balance(buy_exchange)))
            except Exception:
                pass
            self.performance_metrics['missed_trades'] += 1
            logger.warning(f"⚠️ [SIM] Execution exception: {e}")
            return False, 0.0, 0.0, 0.0
        
    def rebalance(self, source_exchange: str, target_exchange: str, amount: float):
        available = self.get_available_balance(source_exchange)
        if available < amount:
            logger.warning(f"[REBALANCE] Insufficient funds on {source_exchange}")
            return False
            
        self.update_balance(source_exchange, -amount, 'available')
        # apply network transfer fee (flat)
        network_fee = 0.5
        credited = max(0.0, amount - network_fee)
        self.update_balance(target_exchange, credited, 'available')
        self.performance_metrics['total_fees_paid'] += network_fee

        logger.info(f"[REBALANCE] Transferred ${amount:.2f} from {source_exchange} to {target_exchange} (fee ${network_fee:.2f})")
        self.last_rebalance = datetime.now()
        return True

    async def rebalance_async(self, source_exchange: str, target_exchange: str, amount: float):
        try:
            delay = random.uniform(2.0, 5.0)
            await asyncio.sleep(delay)
            return self.rebalance(source_exchange, target_exchange, amount)
        except Exception as e:
            logger.debug(f"[REBALANCE] Async rebalance failed: {e}")
            return False
        
    def auto_rebalance(self, threshold_percent: float = 10.0):
        total_balance = self.get_total_balance()
        if total_balance <= 0:
            return
            
        target_per_exchange = total_balance / len(self.balances)
        
        for exchange, balance_data in self.balances.items():
            current_balance = balance_data['total']
            imbalance_percent = abs(current_balance - target_per_exchange) / target_per_exchange * 100
            
            if imbalance_percent > threshold_percent:
                if current_balance > target_per_exchange:
                    amount_to_transfer = current_balance - target_per_exchange
                    for target_exchange, target_balance in self.balances.items():
                        if target_exchange != exchange and target_balance['total'] < target_per_exchange:
                            transfer_amount = min(amount_to_transfer, target_per_exchange - target_balance['total'])
                            if self.rebalance(exchange, target_exchange, transfer_amount):
                                amount_to_transfer -= transfer_amount
                                if amount_to_transfer <= 0:
                                    break

    async def auto_rebalance_async(self, threshold_percent: float = 10.0):
        total_balance = self.get_total_balance()
        if total_balance <= 0:
            return
        target_per_exchange = total_balance / len(self.balances)

        for exchange, balance_data in self.balances.items():
            current_balance = balance_data['total']
            imbalance_percent = abs(current_balance - target_per_exchange) / target_per_exchange * 100
            if imbalance_percent > threshold_percent:
                if current_balance > target_per_exchange:
                    amount_to_transfer = current_balance - target_per_exchange
                    for target_exchange, target_balance in self.balances.items():
                        if target_exchange != exchange and target_balance['total'] < target_per_exchange:
                            transfer_amount = min(amount_to_transfer, target_per_exchange - target_balance['total'])
                            ok = await self.rebalance_async(exchange, target_exchange, transfer_amount)
                            if ok:
                                amount_to_transfer -= transfer_amount
                            if amount_to_transfer <= 0:
                                break

            async def rebalance_wallets_async(self,
                                              min_trade_size: float = 500.0,
                                              imbalance_threshold_pct: float = 15.0,
                                              ignore_delta: float = 100.0,
                                              fee_pct: float = 0.001,
                                              min_fee: float = 0.25):
                """
                Async wrapper for rebalance_wallets.
                """
                try:
                    # small delay to simulate transfer planning
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    return self.rebalance_wallets(min_trade_size, imbalance_threshold_pct, ignore_delta, fee_pct, min_fee)
                except Exception as e:
                    logger.warning(f"⚠️ [REBALANCE] rebalance_wallets_async failed: {e}")
                    return False

            def rebalance_wallets(self,
                                  min_trade_size: float = 500.0,
                                  imbalance_threshold_pct: float = 15.0,
                                  ignore_delta: float = 100.0,
                                  fee_pct: float = 0.001,
                                  min_fee: float = 0.25) -> bool:
                """
                Rebalance core funds according to rules:
                 - Only rebalance when an exchange balance is below min_trade_size or more than imbalance_threshold_pct lower than average.
                 - Transfer only minimum required amount to restore balance to the target.
                 - Ignore small deltas under ignore_delta.
                 - Apply dynamic transfer fee = fee_pct * amount, minimum min_fee; fee deducted from source.
                Returns True if any transfer occurred.
                """
                try:
                    exchanges = list(self.balances.keys())
                    if not exchanges:
                        return False

                    total = self.get_total_balance()
                    avg = total / len(exchanges)

                    # Determine targets and deficits
                    targets = {}
                    deficits = {}
                    for ex in exchanges:
                        bal = self.balances.get(ex, {}).get('total', 0.0)
                        target = max(min_trade_size, avg * (1.0 - imbalance_threshold_pct / 100.0))
                        if bal < target and (target - bal) >= ignore_delta:
                            deficits[ex] = target - bal
                        targets[ex] = target

                    if not deficits:
                        return False

                    # Identify potential sources with surplus
                    surpluses = {}
                    for ex in exchanges:
                        bal = self.balances.get(ex, {}).get('total', 0.0)
                        surplus = bal - avg
                        # only consider surplus above ignore_delta
                        if surplus > ignore_delta:
                            surpluses[ex] = surplus

                    if not surpluses:
                        logger.info("[REBALANCE] No sufficient surplus found to cover deficits")
                        return False

                    # For each deficit, try to source funds from largest surplus exchanges
                    transfers_made = False
                    # Sort surpluses by largest first
                    surplus_list = sorted(surpluses.items(), key=lambda x: x[1], reverse=True)

                    for target_ex, need in deficits.items():
                        remaining_need = need
                        for src_ex, src_surplus in surplus_list:
                            if remaining_need <= 0:
                                break
                            # recalc source available surplus (may have been decreased)
                            src_balance = self.balances.get(src_ex, {}).get('total', 0.0)
                            src_available = src_balance - avg
                            if src_available <= ignore_delta:
                                continue

                            transfer_amount = min(remaining_need, max(0.0, src_available - ignore_delta))
                            if transfer_amount <= 0:
                                continue

                            # apply fee
                            fee = max(min_fee, transfer_amount * fee_pct)

                            # ensure source has enough available to cover fee and transfer
                            src_available_available = self.get_available_balance(src_ex)
                            if src_available_available < (transfer_amount + fee):
                                # if not enough available, reduce transfer_amount
                                possible = max(0.0, src_available_available - fee)
                                if possible <= 0:
                                    continue
                                transfer_amount = min(transfer_amount, possible)
                                fee = max(min_fee, transfer_amount * fee_pct)

                            if transfer_amount < ignore_delta:
                                continue

                            # perform transfer: deduct from source (amount + fee), credit target (amount)
                            self.update_balance(src_ex, -(transfer_amount + fee), 'available')
                            # adjust total on source accordingly
                            self.balances[src_ex]['total'] = self.balances[src_ex]['available'] + self.balances[src_ex]['reserved']

                            self.update_balance(target_ex, transfer_amount, 'available')
                            self.balances[target_ex]['total'] = self.balances[target_ex]['available'] + self.balances[target_ex]['reserved']

                            # fees accounted
                            self.performance_metrics['total_fees_paid'] += fee

                            transfers_made = True
                            remaining_need -= transfer_amount

                            src_surplus -= transfer_amount
                            # update surplus_list in place
                            surplus_list = [(s, v if s != src_ex else (v - transfer_amount)) for s, v in surplus_list]

                            # INFO log per transfer
                            try:
                                logger.info(f"♻️ Rebalanced core funds: Sent ${transfer_amount:.2f} from {src_ex.capitalize()} → {target_ex.capitalize()} (fee ${fee:.2f})")
                            except Exception:
                                logger.info(f"[REBALANCE] Transfer {transfer_amount} from {src_ex} to {target_ex}")

                            # Clamp balances to prevent negatives and sync totals
                            try:
                                for exx in [src_ex, target_ex]:
                                    data = self.balances.get(exx, {})
                                    if data:
                                        for k in ['available', 'reserved']:
                                            if data.get(k, 0.0) < 0:
                                                data[k] = 0.0
                                        data['total'] = data.get('available', 0.0) + data.get('reserved', 0.0)
                            except Exception:
                                pass

                        # finished attempting for this deficit

                    return transfers_made
                except Exception as e:
                    logger.error(f"[REBALANCE] rebalance_wallets failed: {e}")
                    return False
        
    def _update_performance_metrics(self, profit: float, volume: float, success: bool):
        current_balance = self.get_total_balance()
        
        self.performance_metrics['peak_balance'] = max(self.performance_metrics['peak_balance'], current_balance)
        
        drawdown = (self.performance_metrics['peak_balance'] - current_balance) / self.performance_metrics['peak_balance'] * 100
        self.performance_metrics['current_drawdown'] = drawdown
        self.performance_metrics['max_drawdown'] = max(self.performance_metrics['max_drawdown'], drawdown)
        
        self.performance_metrics['total_volume'] += volume
        
        if success:
            if profit > 0:
                self.performance_metrics['successful_trades'] += 1
                self.performance_metrics['total_profit'] += profit
                self.performance_metrics['max_profit'] = max(self.performance_metrics['max_profit'], profit)
                
                today = datetime.now().date()
                if len(self.daily_profits) == 0 or self.daily_profits[-1][0] != today:
                    self.daily_profits.append((today, profit))
                else:
                    last_date, last_profit = self.daily_profits[-1]
                    self.daily_profits[-1] = (last_date, last_profit + profit)
                    
            else:
                self.performance_metrics['failed_trades'] += 1
                self.performance_metrics['max_loss'] = min(self.performance_metrics['max_loss'], profit)
        
        self._calculate_advanced_metrics()
    
    def _calculate_advanced_metrics(self):
        if self.performance_metrics['failed_trades'] > 0:
            self.performance_metrics['profit_factor'] = abs(
                self.performance_metrics['total_profit'] / 
                (self.performance_metrics['total_profit'] - self.performance_metrics['total_profit'])
            )
        
        if len(self.trade_history) > 1:
            profits = [t['profit'] for t in self.trade_history if 'profit' in t]
            if profits:
                avg_profit = sum(profits) / len(profits)
                std_dev = statistics.stdev(profits) if len(profits) > 1 else 0
                self.performance_metrics['sharpe_ratio'] = avg_profit / std_dev if std_dev > 0 else 0
        
        now = datetime.now()
        weekly_profit = 0
        monthly_profit = 0
        
        for trade in self.trade_history:
            trade_time = trade['timestamp'] if isinstance(trade['timestamp'], datetime) else datetime.fromisoformat(trade['timestamp'])
            if (now - trade_time).days <= 7:
                weekly_profit += trade.get('profit', 0)
            if (now - trade_time).days <= 30:
                monthly_profit += trade.get('profit', 0)
                
        self.performance_metrics['weekly_profit'] = weekly_profit
        self.performance_metrics['monthly_profit'] = monthly_profit
    
    def get_total_balance(self) -> float:
        return sum(balance['total'] for balance in self.balances.values())
        
    def get_detailed_balances(self) -> Dict:
        return {
            exchange: {
                'total': data['total'],
                'available': data['available'],
                'reserved': data['reserved'],
                'profit': data['profit']
            }
            for exchange, data in self.balances.items()
        }
        
    def get_performance_stats(self) -> Dict:
        total_trades = self.performance_metrics['successful_trades'] + self.performance_metrics['failed_trades']
        win_rate = self.performance_metrics['successful_trades'] / total_trades if total_trades > 0 else 0
        
        return {
            **self.performance_metrics,
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_profit_per_trade': self.performance_metrics['total_profit'] / total_trades if total_trades > 0 else 0,
            'session_duration': str(datetime.now() - self.session_start),
            'current_balance': self.get_total_balance()
        }     

# ----------------- ML OPPORTUNITY FILTER -----------------
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import joblib

class MLOpportunityFilter:
    def __init__(self, model_path: str = "ml_model.pkl", retrain_interval: int = 100):
        self.opportunity_history = []
        self.min_samples = 100
        self.model_ready = False
        self.model_path = model_path
        self.retrain_interval = retrain_interval
        self.feature_importances_ = None
        
        self.model = Pipeline([
            ('scaler', StandardScaler()),
            ('classifier', RandomForestClassifier(
                n_estimators=100,
                max_depth=5,
                random_state=42,
                class_weight='balanced'
            ))
        ])
        
        self.load_model()
        
    def extract_features(self, opportunity: Dict) -> List[float]:
        try:
            net_spread = opportunity['net_spread_bps'] / 100
            gross_spread = opportunity['gross_spread_bps'] / 100
            total_fee = (opportunity['buy_fee_bps'] + opportunity['sell_fee_bps']) / 200
            log_volume = math.log(opportunity['daily_volume'] + 1)
            volume_millions = opportunity['daily_volume'] / 1000000
            price_diff = abs(opportunity['buy_price'] - opportunity['sell_price']) / opportunity['buy_price'] * 100
            
            current_time = datetime.now()
            hour = current_time.hour / 24
            weekday = current_time.weekday() / 7
            month = current_time.month / 12
            
            symbol = opportunity['symbol']
            symbol_length = len(symbol)
            is_major = int(any(major in symbol for major in ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'SOL', 'DOT']))
            is_stable = int(any(stable in symbol for stable in ['USDT', 'USDC', 'BUSD', 'DAI']))
            
            price_ratio = opportunity['sell_price'] / opportunity['buy_price'] if opportunity['buy_price'] > 0 else 1
            fee_ratio = opportunity['buy_fee_bps'] / opportunity['sell_fee_bps'] if opportunity['sell_fee_bps'] > 0 else 1
            
            features = [
                net_spread,
                gross_spread,
                total_fee,
                log_volume,
                volume_millions,
                price_diff,
                hour,
                weekday,
                month,
                symbol_length,
                is_major,
                is_stable,
                price_ratio,
                fee_ratio,
            ]
            
            return features
        except Exception as e:
            logger.error(f"[ML] Feature extraction error: {e}")
            return [0.0] * 14
    
    def predict_confidence(self, opportunity: Dict) -> float:
        if len(self.opportunity_history) < self.min_samples or not self.model_ready:
            return self._heuristic_confidence(opportunity)
            
        try:
            features = self.extract_features(opportunity)
            X = np.array([features])
            
            confidence = self.model.predict_proba(X)[0, 1]
            
            sample_factor = min(1.0, len(self.opportunity_history) / (self.min_samples * 2))
            confidence = 0.3 + (confidence - 0.3) * sample_factor
            
            return max(0.1, min(0.99, confidence))
        except Exception as e:
            logger.error(f"[ML] Prediction error: {e}")
            return self._heuristic_confidence(opportunity)
    
    def _heuristic_confidence(self, opportunity: Dict) -> float:
        try:
            net_spread = opportunity['net_spread_bps'] / 100
            volume_factor = math.log(opportunity['daily_volume'] + 1) / 10
            
            confidence = 0.5
            
            if net_spread > 0.5:
                confidence += 0.2
            elif net_spread > 0.3:
                confidence += 0.1
            elif net_spread < 0.1:
                confidence -= 0.1
            
            confidence += min(0.2, volume_factor)
            
            hour = datetime.now().hour
            if 9 <= hour <= 17:
                confidence += 0.05
            
            return max(0.1, min(0.99, confidence))
        except:
            return 0.7
    
    def update_model(self, opportunity: Dict, success: bool, actual_profit: float):
        record = {
            'features': self.extract_features(opportunity),
            'success': success and actual_profit > 0,
            'profit': actual_profit,
            'timestamp': datetime.now()
        }
        self.opportunity_history.append(record)
        
        if len(self.opportunity_history) >= self.min_samples and len(self.opportunity_history) % self.retrain_interval == 0:
            self.train_model()
    
    def train_model(self):
        if len(self.opportunity_history) < self.min_samples:
            return
            
        try:
            X = np.array([record['features'] for record in self.opportunity_history])
            y = np.array([1 if record['success'] else 0 for record in self.opportunity_history])
            
            if sum(y) > 0 and sum(y) < len(y):
                self.model.fit(X, y)
                
                if hasattr(self.model.named_steps['classifier'], 'feature_importances_'):
                    self.feature_importances_ = self.model.named_steps['classifier'].feature_importances_
                
                train_accuracy = self.model.score(X, y)
                logger.info(f"[ML] Model trained. Accuracy: {train_accuracy:.3f}, Samples: {len(y)}")
                
                self.model_ready = True
                self.save_model()
                
        except Exception as e:
            logger.error(f"[ML] Training error: {e}")
    
    def save_model(self):
        try:
            joblib.dump({
                'model': self.model,
                'feature_importances': self.feature_importances_,
                'timestamp': datetime.now()
            }, self.model_path)
            logger.info(f"[ML] Model saved to {self.model_path}")
        except Exception as e:
            logger.error(f"[ML] Save model error: {e}")
    
    def load_model(self):
        try:
            if os.path.exists(self.model_path):
                data = joblib.load(self.model_path)
                self.model = data['model']
                self.feature_importances_ = data.get('feature_importances')
                self.model_ready = True
                logger.info(f"[ML] Model loaded from {self.model_path}")
        except Exception as e:
            logger.error(f"[ML] Load model error: {e}")

# ----------------- DYNAMIC RISK MANAGER -----------------
class DynamicRiskManager:
    def __init__(self, config: Config):
        self.config = config
        self.risk_level = 1.0
        self.consecutive_wins = 0
        self.consecutive_losses = 0
        self.max_consecutive_losses = 5
        self.win_streak_bonus = 1.15
        self.loss_penalty = 0.8
        self.volatility_factor = 1.0
        self.risk_history = []
        
    def calculate_position_size(self, opportunity: Dict, available_balance: float, 
                              ml_confidence: float, volatility: float = 1.0) -> float:
        base_size = min(available_balance, self.config.max_trade_usdt)
        
        volatility_factor = 1.0 / max(0.5, min(volatility, 3.0))
        
        confidence_factor = 0.3 + ml_confidence * 0.7
        
        risk_adjusted_size = base_size * self.risk_level * confidence_factor * volatility_factor
        
        min_size = max(10, available_balance * 0.01)
        max_size = min(available_balance, self.config.max_trade_usdt)
        
        final_size = max(min_size, min(max_size, risk_adjusted_size))
        
        logger.debug(f"[RISK] Position size: {final_size:.2f} (base: {base_size:.2f}, "
                    f"risk: {self.risk_level:.2f}, conf: {confidence_factor:.2f}, vol: {volatility_factor:.2f})")
        
        return final_size
    
    def update_risk_level(self, profit: float, trade_quality: float = 1.0):
        self.risk_history.append({
            'timestamp': datetime.now(),
            'profit': profit,
            'risk_level': self.risk_level,
            'quality': trade_quality
        })
        
        if len(self.risk_history) > 100:
            self.risk_history = self.risk_history[-50:]
        
        if profit > 0:
            self.consecutive_wins += 1
            self.consecutive_losses = 0
            
            risk_increase = min(0.2, 0.05 + 0.15 * trade_quality)
            self.risk_level = min(2.0, self.risk_level * (1.0 + risk_increase))
            
        else:
            self.consecutive_losses += 1
            self.consecutive_wins = 0
            
            loss_severity = min(1.0, abs(profit) / (self.config.max_trade_usdt * 0.1))
            risk_decrease = 0.2 + 0.3 * loss_severity
            self.risk_level = max(0.1, self.risk_level * (1.0 - risk_decrease))
            
        if self.consecutive_wins >= 3:
            self.risk_level = min(2.0, self.risk_level * 1.1)
            
        if self.consecutive_losses >= self.max_consecutive_losses:
            self.risk_level = 0.1
            self.consecutive_losses = 0
            
        logger.info(f"[RISK] New risk level: {self.risk_level:.2f} "
                   f"(wins: {self.consecutive_wins}, losses: {self.consecutive_losses})")
    
    def get_risk_stats(self) -> Dict:
        return {
            'current_risk_level': self.risk_level,
            'consecutive_wins': self.consecutive_wins,
            'consecutive_losses': self.consecutive_losses,
            'risk_history_size': len(self.risk_history)
        }

# ----------------- ENHANCED ANALYTICS -----------------
class EnhancedAnalytics:
    def __init__(self):
        self.performance_stats = {
            'total_opportunities': 0,
            'executed_opportunities': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'total_profit': 0.0,
            'total_volume': 0.0,
            'max_drawdown': 0.0,
            'current_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'win_rate': 0.0,
            'avg_profit_per_trade': 0.0,
            'avg_loss_per_trade': 0.0,
            'largest_win': 0.0,
            'largest_loss': 0.0,
            'profit_factor': 0.0,
            'recovery_factor': 0.0,
            'expectancy': 0.0
        }
        self.hourly_performance = {}
        self.daily_performance = {}
        self.symbol_performance = {}
        self.exchange_performance = {}
        self.trade_history = []
        self.peak_balance = 0.0
        self.starting_balance = 0.0
        
    def initialize(self, starting_balance: float):
        self.starting_balance = starting_balance
        self.peak_balance = starting_balance
        
    def update_stats(self, opportunity: Dict, executed: bool, profit: float = 0, volume: float = 0):
        self.performance_stats['total_opportunities'] += 1
        
        if executed:
            self.performance_stats['executed_opportunities'] += 1
            self.performance_stats['total_volume'] += volume
            self.performance_stats['total_profit'] += profit
            
            if profit > 0:
                self.performance_stats['successful_trades'] += 1
                self.performance_stats['largest_win'] = max(self.performance_stats['largest_win'], profit)
            else:
                self.performance_stats['failed_trades'] += 1
                self.performance_stats['largest_loss'] = min(self.performance_stats['largest_loss'], profit)
            
            current_balance = self.starting_balance + self.performance_stats['total_profit']
            self.peak_balance = max(self.peak_balance, current_balance)
            
            drawdown = (self.peak_balance - current_balance) / self.peak_balance * 100
            self.performance_stats['current_drawdown'] = drawdown
            self.performance_stats['max_drawdown'] = max(self.performance_stats['max_drawdown'], drawdown)
            
            hour = datetime.now().hour
            if hour not in self.hourly_performance:
                self.hourly_performance[hour] = {'count': 0, 'profit': 0.0, 'volume': 0.0}
            self.hourly_performance[hour]['count'] += 1
            self.hourly_performance[hour]['profit'] += profit
            self.hourly_performance[hour]['volume'] += volume
            
            today = datetime.now().date()
            if today not in self.daily_performance:
                self.daily_performance[today] = {'count': 0, 'profit': 0.0, 'volume': 0.0}
            self.daily_performance[today]['count'] += 1
            self.daily_performance[today]['profit'] += profit
            self.daily_performance[today]['volume'] += volume
            
            symbol = opportunity['symbol']
            if symbol not in self.symbol_performance:
                self.symbol_performance[symbol] = {'count': 0, 'profit': 0.0, 'volume': 0.0}
            self.symbol_performance[symbol]['count'] += 1
            self.symbol_performance[symbol]['profit'] += profit
            self.symbol_performance[symbol]['volume'] += volume
            
            buy_exchange = opportunity.get('buy_client', '').split('_')[0]
            sell_exchange = opportunity.get('sell_client', '').split('_')[0]
            
            for exchange in [buy_exchange, sell_exchange]:
                if exchange and exchange != 'unknown':
                    if exchange not in self.exchange_performance:
                        self.exchange_performance[exchange] = {'count': 0, 'profit': 0.0, 'volume': 0.0}
                    self.exchange_performance[exchange]['count'] += 0.5
                    self.exchange_performance[exchange]['profit'] += profit * 0.5
                    self.exchange_performance[exchange]['volume'] += volume * 0.5
            
            self._calculate_metrics()
    
    def log_trade(self, trade: Dict):
        self.trade_history.append(trade)
        
    def _calculate_metrics(self):
        """Пересчитывает все метрики производительности"""
        total_trades = self.performance_stats['successful_trades'] + self.performance_stats['failed_trades']
        
        if total_trades > 0:
            # Win Rate
            self.performance_stats['win_rate'] = self.performance_stats['successful_trades'] / total_trades
            
            # Average Profit/Loss per Trade
            if self.performance_stats['successful_trades'] > 0:
                self.performance_stats['avg_profit_per_trade'] = (
                    self.performance_stats['total_profit'] / total_trades
                )
            
            if self.performance_stats['failed_trades'] > 0:
                self.performance_stats['avg_loss_per_trade'] = (
                    (self.performance_stats['total_profit'] - 
                     self.performance_stats['largest_win'] * self.performance_stats['successful_trades']) / 
                    self.performance_stats['failed_trades']
                )
            
            # Profit Factor
            gross_profit = sum(t['profit'] for t in self.trade_history if t.get('profit', 0) > 0)
            gross_loss = abs(sum(t['profit'] for t in self.trade_history if t.get('profit', 0) < 0))
            
            if gross_loss > 0:
                self.performance_stats['profit_factor'] = gross_profit / gross_loss
            
            # Expectancy
            if total_trades > 0:
                avg_win = gross_profit / self.performance_stats['successful_trades'] if self.performance_stats['successful_trades'] > 0 else 0
                avg_loss = gross_loss / self.performance_stats['failed_trades'] if self.performance_stats['failed_trades'] > 0 else 0
                self.performance_stats['expectancy'] = (
                    (self.performance_stats['win_rate'] * avg_win) - 
                    ((1 - self.performance_stats['win_rate']) * avg_loss)
                )
            
            # Recovery Factor
            if self.performance_stats['max_drawdown'] > 0:
                self.performance_stats['recovery_factor'] = (
                    self.performance_stats['total_profit'] / self.performance_stats['max_drawdown']
                )
        
    def get_performance_metrics(self) -> Dict:
        return self.performance_stats.copy()
        
    def get_detailed_report(self) -> Dict:
        return {
            **self.performance_stats,
            'hourly_performance': self.hourly_performance,
            'daily_performance': self.daily_performance,
            'top_performing_symbols': self._get_top_symbols(5),
            'worst_performing_symbols': self._get_worst_symbols(3),
            'exchange_performance': self.exchange_performance
        }
    
    def _get_top_symbols(self, n: int) -> List[Dict]:
        profitable = {k: v for k, v in self.symbol_performance.items() if v['profit'] > 0}
        sorted_symbols = sorted(profitable.items(), key=lambda x: x[1]['profit'], reverse=True)
        return sorted_symbols[:n]
    
    def _get_worst_symbols(self, n: int) -> List[Dict]:
        unprofitable = {k: v for k, v in self.symbol_performance.items() if v['profit'] < 0}
        sorted_symbols = sorted(unprofitable.items(), key=lambda x: x[1]['profit'])
        return sorted_symbols[:n]
    
    def export_to_csv(self, filename: str):
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'timestamp', 'symbol', 'buy_exchange', 'sell_exchange', 
                    'volume', 'profit', 'buy_price', 'sell_price', 'fees',
                    'gross_spread', 'net_spread', 'duration'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for trade in self.trade_history:
                    writer.writerow(trade)
            logger.info(f"[ANALYTICS] Exported {len(self.trade_history)} trades to {filename}")
        except Exception as e:
            logger.error(f"[ANALYTICS] Export error: {e}")

# ----------------- LIQUIDITY CHECKER v2 -----------------
class LiquidityCheckerV2:
    def __init__(self, cache_timeout: int = 300, max_cache_size: int = 1000):
        self.liquidity_cache = {}
        self.cache_timeout = cache_timeout
        self.max_cache_size = max_cache_size
        self.last_cleanup = time.time()
        
    async def check_liquidity(self, symbol: str, exchange_client: ExchangeClient, 
                             required_amount: float, max_slippage: float = 0.01) -> Tuple[bool, float, float]:
        try:
            cache_key = f"{symbol}_{exchange_client.exchange_name}_{required_amount}_{max_slippage}"
            current_time = time.time()
            
            if cache_key in self.liquidity_cache:
                cached_data = self.liquidity_cache[cache_key]
                if current_time - cached_data['timestamp'] < self.cache_timeout:
                    return (cached_data['has_liquidity'], 
                            cached_data['available_volume'], 
                            cached_data['estimated_slippage'])
            
            order_book = await exchange_client.fetch_order_book(symbol, 100)
            
            if not order_book or not order_book.get('asks') or not order_book.get('bids'):
                return False, 0.0, 0.0
            
            buy_liquidity, buy_slippage = self._calculate_liquidity_with_slippage(
                order_book['asks'], required_amount, True, max_slippage
            )
            
            sell_liquidity, sell_slippage = self._calculate_liquidity_with_slippage(
                order_book['bids'], required_amount, False, max_slippage
            )
            
            has_liquidity = (buy_liquidity >= required_amount and 
                            sell_liquidity >= required_amount and
                            buy_slippage <= max_slippage and 
                            sell_slippage <= max_slippage)
            
            available_volume = min(buy_liquidity, sell_liquidity)
            estimated_slippage = max(buy_slippage, sell_slippage)
            
            self.liquidity_cache[cache_key] = {
                'has_liquidity': has_liquidity,
                'available_volume': available_volume,
                'estimated_slippage': estimated_slippage,
                'timestamp': current_time
            }
            
            self._cleanup_cache()
            
            return has_liquidity, available_volume, estimated_slippage
            
        except Exception as e:
            logger.error(f"[LIQUIDITY] Error checking liquidity for {symbol}: {e}")
            return False, 0.0, 0.0
            
    def _calculate_liquidity_with_slippage(self, orders: List[List[float]], required_amount: float, 
                                          is_buy: bool, max_slippage: float) -> Tuple[float, float]:
        if not orders:
            return 0.0, 0.0
            
        total_volume = 0.0
        total_cost = 0.0
        initial_price = orders[0][0]
        weighted_price = 0.0
        
        for order in orders:
            price, volume = order[0], order[1]
            order_cost = price * volume
            
            if total_cost + order_cost >= required_amount:
                needed_cost = required_amount - total_cost
                needed_volume = needed_cost / price
                total_volume += needed_volume
                total_cost += needed_cost
                weighted_price += price * needed_volume
                break
            else:
                total_volume += volume
                total_cost += order_cost
                weighted_price += price * volume
        
        if total_volume == 0:
            return 0.0, 0.0
        
        average_price = weighted_price / total_volume
        
        if initial_price > 0:
            if is_buy:
                slippage = (average_price - initial_price) / initial_price
            else:
                slippage = (initial_price - average_price) / initial_price
        else:
            slippage = 0.0
        
        return total_volume, abs(slippage)
    
    def _cleanup_cache(self):
        current_time = time.time()
        
        if current_time - self.last_cleanup < 300:
            return
            
        expired_keys = []
        for key, data in self.liquidity_cache.items():
            if current_time - data['timestamp'] > self.cache_timeout:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.liquidity_cache[key]
        
        if len(self.liquidity_cache) > self.max_cache_size:
            sorted_keys = sorted(
                self.liquidity_cache.items(),
                key=lambda x: x[1]['timestamp']
            )
            for key, _ in sorted_keys[:self.max_cache_size // 2]:
                del self.liquidity_cache[key]
        
        self.last_cleanup = current_time

# ----------------- ARBITRAGE ENGINE v9.1 -----------------
class ArbitrageEngineV9_1:
    def __init__(self, config: Config):
        self.config = config
        self.clients = {}
        self.telegram = TelegramNotifier(config.telegram_token, config.telegram_chat_id, config.telegram_min_alert_interval)
        self.contract_validator = ContractValidator()
        self.wallet = VirtualWalletV3(config.initial_balance)
        self.ml_filter = MLOpportunityFilter()
        self.risk_manager = DynamicRiskManager(config)
        self.analytics = EnhancedAnalytics()
        self.liquidity_checker = LiquidityCheckerV2()
        self.performance_monitor = PerformanceMonitor()
        
        self.is_running = False
        self.opportunities_found = 0
        self.start_time = time.time()
        self.last_status_time = 0
        self.last_balance_warning_time = {}
        self.symbol_blacklist = set()
        self.delisted_tokens = set()
        self.trades_this_cycle = 0
        self.cycle_start_time = time.time()
        self.pair_cooldown = {}
        self.symbol_priority_list = set()
        self.last_symbol_refresh = 0
        self._virtual_report_task = None
        
    async def initialize(self):
        logger.info("[START] Initializing Arbitrage Bot v9.1")
        # If running in paper_trading, force virtual mode to ensure synthetic markets/tickers are used
        global VIRTUAL_MODE
        if self.config.paper_trading:
            VIRTUAL_MODE = True
        
        for exchange in self.config.exchanges:
            for market_type in self.config.market_types:
                client_key = f"{exchange}_{market_type}"
                try:
                    self.clients[client_key] = ExchangeClient(exchange, market_type)
                    await self.clients[client_key].initialize()
                except Exception as e:
                    logger.error(f"[ERROR] Failed to create client {client_key}: {e}")

        # If running in virtual mode and clients have no markets (no ccxt), seed synthetic markets
        if VIRTUAL_MODE:
            try:
                await self._seed_virtual_markets()
            except Exception:
                logger.debug("[VIRTUAL] Failed to seed virtual markets", exc_info=True)
        # relax thresholds for virtual testing
        if self.config.paper_trading:
            try:
                self.config.min_net_spread_bps = min(self.config.min_net_spread_bps, 5)
                self.config.min_daily_volume = min(self.config.min_daily_volume, 1000)
            except Exception:
                pass
        
        await self.load_delisted_tokens()
        await self.load_priority_symbols()
        
        self.wallet.initialize_balances(self.config.exchanges)
        telegram_initialized = await self.telegram.start()
        
        if not telegram_initialized:
            logger.warning("[WARNING] Telegram notifier not initialized. Notifications disabled.")
        
        logger.info("[OK] All components initialized")
        # start periodic virtual report every 5 minutes
        try:
            self._virtual_report_task = asyncio.create_task(self._virtual_reporter())
        except Exception:
            logger.debug("[REPORT] Failed to start virtual reporter task", exc_info=True)
    
    async def load_delisted_tokens(self):
        self.delisted_tokens = set()
        logger.info("[DELISTED] No delisted tokens loaded")

    async def _seed_virtual_markets(self):
        # Create a small set of tokens with slightly different prices across exchanges
        tokens = [
            'LUMIA/USDT', 'WHY/USDT', 'ANLOG/USDT', 'SCA/USDT', 'TURBOS/USDT', 'AVAAI/USDT',
            'MBOX/USDT', 'TOMA/USDT', 'BBQ/USDT'
        ]
        base_prices = {t: random.uniform(0.01, 5.0) for t in tokens}

        for client_key, client in self.clients.items():
            # only seed if markets empty
            if not client.markets:
                client.markets = {}
                for t in tokens:
                    price = base_prices[t] * random.uniform(0.995, 1.005)
                    client.markets[t] = {
                        'symbol': t,
                        'price': round(price, 8),
                        'active': True,
                        'base': t.split('/')[0],
                        'quote': 'USDT',
                        'quoteVolume': random.uniform(5000, 60000),
                        'baseVolume': random.uniform(1000, 50000)
                    }
    
    async def load_priority_symbols(self):
        try:
            volume_data = {}
            
            for client_name, client in self.clients.items():
                try:
                    tickers = await client.fetch_tickers()
                    if tickers:
                        for symbol, ticker in tickers.items():
                            if '/USDT' in symbol and not self.is_option_symbol(symbol):
                                volume = ticker.get('quoteVolume', 0)
                                if volume > self.config.min_daily_volume:
                                    volume_data[symbol] = volume_data.get(symbol, 0) + volume
                except Exception as e:
                    logger.error(f"[PRIORITY] Error loading symbols from {client_name}: {e}")
            
            sorted_symbols = sorted(volume_data.items(), key=lambda x: x[1], reverse=True)
            self.symbol_priority_list = {symbol for symbol, volume in sorted_symbols[:self.config.max_priority_symbols]}
            
            logger.info(f"[PRIORITY] Loaded {len(self.symbol_priority_list)} priority symbols")
        except Exception as e:
            logger.error(f"[PRIORITY] Failed to load priority symbols: {e}")
            self.symbol_priority_list = self.get_all_symbols()

    async def _virtual_reporter(self):
        """Periodically log the virtual trading report every 5 minutes (300s)."""
        try:
            while True:
                try:
                    await asyncio.sleep(300)
                    stats = self.wallet.get_performance_stats()
                    total = stats.get('successful_trades', 0) + stats.get('failed_trades', 0)
                    successful = stats.get('successful_trades', 0)
                    failed = stats.get('failed_trades', 0)
                    profit = stats.get('total_profit', 0.0)
                    balance = stats.get('current_balance', self.wallet.get_total_balance())
                    avg_spread = 0.0
                    if self.performance_monitor.opportunities_per_cycle:
                        avg_spread = statistics.mean(self.performance_monitor.opportunities_per_cycle)

                    report = (
                        "🤖 Виртуальный отчёт:\n"
                        f"Время работы: {str(datetime.now() - datetime.fromtimestamp(self.start_time)).split('.')[0]}\n"
                        f"Совершено сделок: {total}\n"
                        f"Успешные: {successful}\n"
                        f"Неудачные: {failed}\n"
                        f"Общая прибыль: ${profit:.2f}\n"
                        f"Текущий баланс: ${balance:.2f}\n"
                        f"Средний спред: {avg_spread:.2f}\n"
                    )
                    logger.info(report)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"[REPORT] Reporter error: {e}")
                    await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
    
    async def refresh_priority_symbols(self):
        current_time = time.time()
        if current_time - self.last_symbol_refresh > self.config.symbol_refresh_interval:
            await self.load_priority_symbols()
            self.last_symbol_refresh = current_time
    
    def get_all_symbols(self) -> Set[str]:
        all_symbols = set()
        for client in self.clients.values():
            for symbol in client.markets.keys():
                if '/USDT' in symbol and not self.is_option_symbol(symbol):
                    all_symbols.add(symbol)
        return all_symbols
    
    def is_option_symbol(self, symbol: str) -> bool:
        return any(x in symbol.upper() for x in ['-C-', '-P-', '-CALL-', '-PUT-', 'OPTION'])
    
    async def get_symbols_to_scan(self) -> List[str]:
        await self.refresh_priority_symbols()
        
        if not self.symbol_priority_list:
            return list(self.get_all_symbols())[:self.config.max_symbols_to_scan]
        
        priority_symbols = list(self.symbol_priority_list)
        other_symbols = list(self.get_all_symbols() - self.symbol_priority_list)
        
        max_other_symbols = min(len(other_symbols), 
                               self.config.max_symbols_to_scan - len(priority_symbols))
        
        if max_other_symbols > 0:
            selected_other = random.sample(other_symbols, max_other_symbols)
            return priority_symbols + selected_other
        
        return priority_symbols[:self.config.max_symbols_to_scan]
    
    async def scan_symbol(self, symbol: str) -> Optional[Dict]:
        try:
            prices = {}
            volumes = {}
            fees = {}
            
            for client_name, client in self.clients.items():
                ticker = await client.fetch_ticker(symbol)
                if ticker and ticker.get('last') and ticker.get('last') > 0:
                    prices[client_name] = ticker['last']
                    # use client helper to derive quote volume robustly
                    try:
                        volumes[client_name] = client._extract_quote_volume_from_ticker(ticker)
                    except Exception:
                        volumes[client_name] = ticker.get('quoteVolume', 0) or 0
                    fees[client_name] = client.fee_bps
            
            if len(prices) < 2:
                logger.debug(f"[SCAN:{symbol}] Insufficient price sources: found {len(prices)} prices")
                return None

            # Sanity filter: remove extreme outliers compared to median price
            try:
                price_values = list(prices.values())
                med = statistics.median(price_values)
                filtered_prices = {}
                for k, v in prices.items():
                    try:
                        if v and v > 0 and (0.01 * med) <= v <= (100.0 * med):
                            filtered_prices[k] = v
                    except Exception:
                        continue
                if len(filtered_prices) < 2:
                    logger.warning(f"[SCAN:{symbol}] Price outliers removed or insufficient valid prices after filtering (median={med})")
                    return None
                prices = filtered_prices
            except Exception as e:
                logger.debug(f"[SCAN:{symbol}] Price sanity filtering failed: {e}")
            
            best_bid_client = max(prices.items(), key=lambda x: x[1])[0]
            best_ask_client = min(prices.items(), key=lambda x: x[1])[0]
            
            if best_bid_client == best_ask_client:
                logger.debug(f"[SCAN:{symbol}] Best bid and ask from same client {best_bid_client}, skipping")
                return None
            
            best_bid_price = prices[best_bid_client]
            best_ask_price = prices[best_ask_client]
            
            # Compute spreads in percent (safer to reason about). Convert fees (bps) to percent.
            gross_spread_pct = ((best_bid_price - best_ask_price) / best_ask_price) * 100.0
            total_fee_bps = fees[best_bid_client] + fees[best_ask_client]
            total_fee_pct = total_fee_bps / 100.0
            net_spread_pct = gross_spread_pct - total_fee_pct
            gross_spread_bps = gross_spread_pct * 100.0
            net_spread_bps = net_spread_pct * 100.0

            if net_spread_bps < self.config.min_net_spread_bps:
                logger.warning(f"[SCAN:{symbol}] Reject: net spread {net_spread_bps:.2f} bps < min {self.config.min_net_spread_bps} bps (gross_pct={gross_spread_pct:.4f}%, fees_pct={total_fee_pct:.4f}%)")
                return None

            # Conservative daily volume check: require sufficient volume on both sides
            buy_vol = volumes.get(best_ask_client, 0)
            sell_vol = volumes.get(best_bid_client, 0)
            daily_volume = min(buy_vol if buy_vol else float('inf'), sell_vol if sell_vol else 0)
            # fallback to whichever available
            if daily_volume == float('inf'):
                daily_volume = max(buy_vol, sell_vol, 0)

            if daily_volume < self.config.min_daily_volume:
                logger.warning(f"[SCAN:{symbol}] Reject: daily volume {daily_volume:.2f} < min {self.config.min_daily_volume}")
                return None
            
            # Defensive: ensure client objects expose get_market_info
            bid_client_obj = self.clients.get(best_bid_client)
            ask_client_obj = self.clients.get(best_ask_client)

            if bid_client_obj is None or ask_client_obj is None:
                logger.warning(f"[MARKET_MISS:{symbol}] Missing client object for bid={best_bid_client} or ask={best_ask_client}")
                return None

            if not hasattr(bid_client_obj, 'get_market_info') or not callable(getattr(bid_client_obj, 'get_market_info')):
                logger.warning(f"[MARKET_MISS:{symbol}] Client {best_bid_client} has no get_market_info method; type={type(bid_client_obj)}; keys={getattr(bid_client_obj, 'markets', None)}")
                return None

            if not hasattr(ask_client_obj, 'get_market_info') or not callable(getattr(ask_client_obj, 'get_market_info')):
                logger.warning(f"[MARKET_MISS:{symbol}] Client {best_ask_client} has no get_market_info method; type={type(ask_client_obj)}; keys={getattr(ask_client_obj, 'markets', None)}")
                return None

            bid_market = bid_client_obj.get_market_info(symbol)
            ask_market = ask_client_obj.get_market_info(symbol)
            
            if not bid_market or not ask_market:
                # log attempted keys to help diagnose format mismatch
                try:
                    bid_keys = self.clients[best_bid_client]._find_market_key_variants(symbol)
                    ask_keys = self.clients[best_ask_client]._find_market_key_variants(symbol)
                    logger.warning(f"[MARKET_MISS:{symbol}] bid_client={best_bid_client} tried keys={bid_keys}; ask_client={best_ask_client} tried keys={ask_keys}")
                except Exception:
                    logger.warning(f"[MARKET_MISS:{symbol}] Missing market info for {best_bid_client} or {best_ask_client}")
                return None
            
            valid_contracts = await self.contract_validator.validate_contracts(symbol, bid_market, ask_market)
            if not valid_contracts:
                # relax for spot markets: if both markets indicate spot, allow
                try:
                    if bid_market.get('type') == 'spot' and ask_market.get('type') == 'spot':
                        logger.info(f"[SCAN:{symbol}] Contract validation relaxed for spot markets")
                    else:
                        logger.warning(f"[SCAN:{symbol}] Contract validation failed between {best_bid_client} and {best_ask_client}")
                        return None
                except Exception:
                    logger.warning(f"[SCAN:{symbol}] Contract validation failed and could not relax")
                    return None
            
            opportunity = {
                'symbol': symbol,
                'buy_client': best_ask_client,
                'sell_client': best_bid_client,
                'buy_price': best_ask_price,
                'sell_price': best_bid_price,
                'gross_spread_pct': gross_spread_pct,
                'net_spread_pct': net_spread_pct,
                'gross_spread_bps': gross_spread_bps,
                'net_spread_bps': net_spread_bps,
                'buy_fee_bps': fees[best_ask_client],
                'sell_fee_bps': fees[best_bid_client],
                'daily_volume': daily_volume,
                'timestamp': datetime.now()
            }
            
            # concise spread output for console
            # Spread lines suppressed per user preference (do not emit SPREAD lines)

            return opportunity
            
        except Exception as e:
            # Log full traceback to help diagnosis (will include NameError, AttributeError, etc.)
            logger.exception(f"[SCAN] Error scanning symbol {symbol}: {e}")
            return None
    
    async def scan_opportunities(self) -> List[Dict]:
        symbols = await self.get_symbols_to_scan()
        opportunities = []
        
        semaphore = asyncio.Semaphore(self.config.max_concurrent_scans)
        
        async def scan_with_semaphore(symbol):
            async with semaphore:
                try:
                    return await self.scan_symbol(symbol)
                except Exception as e:
                    logger.error(f"[SCAN] Error scanning {symbol}: {e}")
                    return None
        
        chunk_size = 50
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]
            tasks = [scan_with_semaphore(symbol) for symbol in chunk]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Dict):
                    opportunities.append(result)
            
            if i + chunk_size < len(symbols):
                await asyncio.sleep(1)
        
        return opportunities
    
    def is_pair_in_cooldown(self, symbol: str) -> bool:
        if symbol in self.pair_cooldown:
            cooldown_end = self.pair_cooldown[symbol]
            if datetime.now() < cooldown_end:
                return True
            else:
                del self.pair_cooldown[symbol]
        return False
    
    def set_pair_cooldown(self, symbol: str):
        cooldown_end = datetime.now() + timedelta(minutes=self.config.pair_cooldown_minutes)
        self.pair_cooldown[symbol] = cooldown_end
    
    def _cleanup_cooldowns(self):
        current_time = datetime.now()
        expired_symbols = []
        
        for symbol, cooldown_end in self.pair_cooldown.items():
            if current_time > cooldown_end:
                expired_symbols.append(symbol)
        
        for symbol in expired_symbols:
            del self.pair_cooldown[symbol]
    
    async def check_liquidity_for_opportunity(self, opportunity: Dict, volume: float) -> Tuple[bool, float]:
        try:
            buy_client = self.clients[opportunity['buy_client']]
            sell_client = self.clients[opportunity['sell_client']]
            
            buy_has_liquidity, buy_available, buy_slippage = await self.liquidity_checker.check_liquidity(
                opportunity['symbol'], buy_client, volume, self.config.max_slippage_percent / 100
            )
            
            sell_has_liquidity, sell_available, sell_slippage = await self.liquidity_checker.check_liquidity(
                opportunity['symbol'], sell_client, volume, self.config.max_slippage_percent / 100
            )
            
            has_liquidity = buy_has_liquidity and sell_has_liquidity
            available_volume = min(buy_available, sell_available)
            
            return has_liquidity, available_volume
            
        except Exception as e:
            logger.error(f"[LIQUIDITY] Error checking liquidity for {opportunity['symbol']}: {e}")
            return False, 0.0
    
    def format_trade_message(self, opportunity: Dict, profit: float, ml_confidence: float, volume: float) -> str:
        net_pct = opportunity.get('net_spread_pct')
        if net_pct is None:
            net_pct = opportunity.get('net_spread_bps', 0) / 100.0

        return (
            f"💰 <b>ARBITRAGE TRADE EXECUTED</b>\n\n"
            f"🎯 <b>Symbol:</b> {opportunity['symbol']}\n"
            f"🛒 <b>Buy:</b> {opportunity['buy_client'].split('_')[0].upper()} at ${opportunity['buy_price']:.4f}\n"
            f"🏪 <b>Sell:</b> {opportunity['sell_client'].split('_')[0].upper()} at ${opportunity['sell_price']:.4f}\n"
            f"📊 <b>Volume:</b> ${volume:.2f}\n"
            f"💵 <b>Profit:</b> ${profit:.2f}\n"
            f"📈 <b>Net Spread:</b> {net_pct:.4f}%\n"
            f"🤖 <b>ML Confidence:</b> {ml_confidence:.1%}\n"
            f"⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}"
        )

    def format_detailed_trade_message_oldstyle(self, opportunity: Dict, profit: float, ml_confidence: float,
                                               volume: float, fill_ratio: float, total_fees: float) -> str:
        """Compose the detailed Russian message for Telegram matching the sample.

        Includes trade lines, wallet summary and the status block. Used for virtual trading
        notifications (paper_trading).
        """
        buy_name = opportunity['buy_client'].split('_')[0].upper()
        sell_name = opportunity['sell_client'].split('_')[0].upper()
        gross_pct = opportunity.get('gross_spread_pct', opportunity.get('gross_spread_bps', 0) / 100.0)
        net_pct = opportunity.get('net_spread_pct', opportunity.get('net_spread_bps', 0) / 100.0)

        # Wallet stats
        stats = self.wallet.get_performance_stats()
        current_balance = round(stats.get('current_balance', self.wallet.get_total_balance()), 2)
        total_profit = round(stats.get('total_profit', 0.0), 2)
        successful = stats.get('successful_trades', 0)
        failed = stats.get('failed_trades', 0)
        total_trades = successful + failed if (successful + failed) > 0 else successful

        # Compose message in the old requested Russian style
        message = (
            f"🎯 Пара: {opportunity['symbol']}\n"
            f"📊 Тип: {opportunity.get('buy_market_type','SPOT').upper()}\n"
            f"🛒 Покупка: {buy_name} (${opportunity['buy_price']:.4f})\n"
            f"💰 Продажа: {sell_name} (${opportunity['sell_price']:.4f})\n"
            f"📈 Грязный спред: {gross_pct:.2f}%\n"
            f"🧹 Чистый спред: {net_pct:.2f}%\n"
            f"✅ Реальная прибыль: {((profit / volume) * 100) if volume>0 else 0.0:.2f}%\n"
            f"🧠 ML Уверенность: {ml_confidence:.2f}\n"
            f"💵 Размер сделки: ${volume:.2f}\n"
            f"💰 Прибыль: ${profit:.2f}\n\n"
            f"📊 Баланс: ${current_balance:.2f}\n"
            f"📈 Общая прибыль: ${total_profit:.2f}\n"
            f"🎯 Успешность: {successful}/{total_trades}\n"
            f"🚨 ВИРТУАЛЬНАЯ ТОРГОВЛЯ"
        )

        return message
    
    async def process_opportunity(self, opportunity: Dict):
        if self.trades_this_cycle >= self.config.max_trades_per_cycle:
            logger.info(f"[LIMIT] Max trades per cycle reached ({self.config.max_trades_per_cycle})")
            return
            
        if self.is_pair_in_cooldown(opportunity['symbol']):
            logger.debug(f"[COOLDOWN] {opportunity['symbol']} is in cooldown")
            return
        
        # Use a fixed trade size for virtual trading to keep numbers stable
        buy_exchange = opportunity['buy_client'].split('_')[0]
        fixed_trade_size = min(self.config.max_trade_usdt, 600.0)
        available_balance = self.wallet.get_available_balance(buy_exchange)

        # Ensure we don't attempt to trade more than available
        trade_volume = min(fixed_trade_size, available_balance)
        
        if trade_volume < self.config.min_trade_usdt:
            current_time = time.time()
            last_warning = self.last_balance_warning_time.get(buy_exchange, 0)
            
            if current_time - last_warning > self.config.balance_warning_interval:
                logger.warning(f"[BALANCE] Insufficient funds on {buy_exchange}: ${available_balance:.2f} < ${self.config.min_trade_usdt:.2f}")
                self.last_balance_warning_time[buy_exchange] = current_time
            return
    
        has_liquidity, available_volume = await self.check_liquidity_for_opportunity(opportunity, trade_volume)
        
        if not has_liquidity or available_volume < trade_volume:
            trade_volume = min(trade_volume, available_volume)
            
            if trade_volume < self.config.min_trade_usdt:
                logger.debug(f"[LIQUIDITY] Insufficient liquidity for {opportunity['symbol']}")
                return
        
        ml_confidence = self.ml_filter.predict_confidence(opportunity)
        
        if ml_confidence < self.config.ml_confidence_threshold:
            logger.debug(f"[ML] Opportunity rejected: {opportunity['symbol']} confidence: {ml_confidence:.2f}")
            return
            
        if self.config.real_trading:
            net_profit_percent = (opportunity['net_spread_bps'] / 100)
            if net_profit_percent < self.config.min_net_profit_percent:
                logger.debug(f"[PROFIT] Net profit {net_profit_percent:.2f}% < minimum {self.config.min_net_profit_percent:.2f}%")
                return
        
        trade_id = self.performance_monitor.start_trade(opportunity, trade_volume)

        success = False
        profit = 0.0
        fill_ratio = 0.0
        total_fees = 0.0

        try:
            if self.config.paper_trading:
                buy_client_obj = self.clients.get(opportunity['buy_client'])
                sell_client_obj = self.clients.get(opportunity['sell_client'])
                sim_res = await self.wallet.execute_trade_simulated_async(
                    opportunity['symbol'],
                    buy_exchange,
                    opportunity['sell_client'].split('_')[0],
                    opportunity['buy_price'],
                    opportunity['sell_price'],
                    trade_volume,
                    opportunity['buy_fee_bps'],
                    opportunity['sell_fee_bps'],
                    buy_client=buy_client_obj.client if buy_client_obj else None,
                    sell_client=sell_client_obj.client if sell_client_obj else None,
                    liquidity_available=None
                )
                # simulate returns: (success, profit, fill_ratio, total_fees)
                success, profit, fill_ratio, total_fees = sim_res
                # Update aggregated wallet metrics exactly once here
                if success:
                    # update aggregated metrics in wallet.performance_metrics in a controlled way
                    self.wallet.performance_metrics['total_profit'] += profit
                    self.wallet.performance_metrics['total_volume'] += trade_volume * (fill_ratio if fill_ratio else 1.0)
                    self.wallet.performance_metrics['trade_count'] += 1
                    if profit > 0:
                        self.wallet.performance_metrics['successful_trades'] += 1
                    else:
                        self.wallet.performance_metrics['failed_trades'] += 1
                    self.wallet.performance_metrics['total_fees_paid'] += total_fees
            else:
                # legacy synchronous execution
                exec_res = self.wallet.execute_trade(
                    buy_exchange,
                    opportunity['sell_client'].split('_')[0],
                    opportunity['buy_price'],
                    opportunity['sell_price'],
                    trade_volume,
                    opportunity['buy_fee_bps'],
                    opportunity['sell_fee_bps']
                )
                # execute_trade now returns (success, profit, fees) in updated code-path
                if isinstance(exec_res, tuple) and len(exec_res) >= 2:
                    success = exec_res[0]
                    profit = exec_res[1]
                    total_fees = exec_res[2] if len(exec_res) > 2 else 0.0
                fill_ratio = 1.0
        except Exception as ex:
            # Log a compact error block without stopping the bot
            err_msg = (
                f"⚠️ Ошибка сделки\n"
                f"Пара: {opportunity.get('symbol')}\n"
                f"Причина: {str(ex)}\n"
                f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            logger.error(err_msg)
            success = False

        self.performance_monitor.end_trade(trade_id, success, profit)
        
        if success:
            # update cycle/trade analytics
            self.trades_this_cycle += 1
            self.set_pair_cooldown(opportunity['symbol'])
            self.ml_filter.update_model(opportunity, profit > 0, profit)
            self.risk_manager.update_risk_level(profit)
            filled_volume = trade_volume * (fill_ratio if fill_ratio else 1.0)
            self.analytics.update_stats(opportunity, True, profit, filled_volume)

            trade_record = {
                'timestamp': datetime.now(),
                'symbol': opportunity['symbol'],
                'buy_exchange': buy_exchange,
                'sell_exchange': opportunity['sell_client'].split('_')[0],
                'volume': trade_volume,
                'profit': profit,
                'buy_price': opportunity['buy_price'],
                'sell_price': opportunity['sell_price'],
                'gross_spread': opportunity.get('gross_spread_pct', opportunity.get('gross_spread_bps', 0)/100.0),
                'net_spread': opportunity.get('net_spread_pct', opportunity.get('net_spread_bps', 0)/100.0),
                'ml_confidence': ml_confidence
            }
            self.analytics.log_trade(trade_record)

            # Build a single concise user-facing message for both profit and loss cases
            try:
                buy_name = opportunity['buy_client'].split('_')[0].capitalize()
                sell_name = opportunity['sell_client'].split('_')[0].capitalize()
                buy_exchange = buy_name.upper()
                sell_exchange = sell_name.upper()

                filled_volume = trade_volume * (fill_ratio if fill_ratio else 1.0)
                volume_usd = filled_volume
                fee_percent = (opportunity.get('buy_fee_bps', 0) + opportunity.get('sell_fee_bps', 0)) / 100.0
                profit_usd = profit
                profit_percent = (profit_usd / volume_usd * 100) if volume_usd > 0 else 0.0

                buy_balance = self.wallet.get_total_balance(buy_exchange)
                sell_balance = self.wallet.get_total_balance(sell_exchange)

                message = (
                    f"💹 Сделка: {buy_exchange} → {sell_exchange}\n"
                    f"Пара: {opportunity['symbol']}\n"
                    f"Покупка: {opportunity['buy_price']:.4f} | Продажа: {opportunity['sell_price']:.4f}\n"
                    f"Объём: ${volume_usd:,.0f}\n"
                    f"Комиссия: {fee_percent:.1f}%\n"
                    f"Прибыль: {('+' if profit_usd>=0 else '-')}${abs(profit_usd):.2f} ({profit_percent:+.3f}%)\n"
                    f"Баланс {buy_name.upper()}: {buy_balance:.2f} USDT\n"
                    f"Баланс {sell_name.upper()}: {sell_balance:.2f} USDT"
                )

                logger.info(message)
                if self.telegram:
                    await self.telegram.send_message(message)
            except Exception:
                logger.exception("[POSTTRADE] Failed to build/send trade message")

    def format_status_message(self) -> str:
        try:
            stats = self.wallet.get_performance_stats()
        except Exception:
            stats = {}

        perf_stats = self.performance_monitor.get_stats()

        uptime_seconds = time.time() - getattr(self, 'start_time', time.time())
        uptime_str = str(timedelta(seconds=int(uptime_seconds)))

        total_trades = perf_stats.get('total_trades', 0)
        successful = perf_stats.get('successful_trades', 0)
        winrate = (successful / total_trades) if total_trades > 0 else 0.0

        avg_cycle_time = perf_stats.get('avg_cycle_time', 0.0)
        avg_ops = perf_stats.get('avg_opportunities_per_cycle', 0.0)

        total_profit = self.wallet.performance_metrics.get('total_profit', 0.0)
        current_balance = self.wallet.get_total_balance()

        return (
            f"🤖 <b>СТАТУС БОТА v9.1</b>\n\n"
            f"⏱️ <b>Время работы:</b> {uptime_str}\n"
            f"✅ <b>Успешные сделки:</b> {successful}\n"
            f"💵 <b>Общая прибыль:</b> ${total_profit:.2f}\n"
            f"📈 <b>Текущий баланс:</b> ${current_balance:.2f}\n"
            f"🎯 <b>Винрейт:</b> {winrate:.2%}\n"
            f"⚡ <b>Среднее время цикла:</b> {avg_cycle_time:.2f}s\n"
            f"🔍 <b>Возможностей/цикл:</b> {avg_ops:.1f}\n\n"
            f"<i>Размер сделки (фикс): ${min(self.config.max_trade_usdt, 600.0):.2f}</i>"
        )
    
    async def run(self):
        self.is_running = True
        logger.info("[SCAN] Starting arbitrage scanner")
        
        self.last_status_time = time.time()
        cycle_count = 0
        
        try:
            while self.is_running:
                cycle_start_time = time.time()
                cycle_count += 1
                
                try:
                    self.trades_this_cycle = 0
                    
                    opportunities = await self.scan_opportunities()
                    
                    if opportunities:
                        # remain silent at INFO level during scanning; keep debug for diagnostics
                        logger.debug(f"[FOUND] {len(opportunities)} opportunities in cycle {cycle_count}")

                        opportunities.sort(key=lambda x: x['net_spread_bps'], reverse=True)

                        for opportunity in opportunities[:self.config.max_trades_per_cycle]:
                            if self.trades_this_cycle < self.config.max_trades_per_cycle:
                                await self.process_opportunity(opportunity)
                            else:
                                break
                    
                    # send periodic status messages at configured Telegram alert interval
                    current_time = time.time()
                    if current_time - self.last_status_time > self.config.telegram_min_alert_interval:
                        self.last_status_time = current_time
                        try:
                            status_msg = self.format_status_message()
                            if self.telegram:
                                await self.telegram.send_message(status_msg)
                        except Exception:
                            logger.debug("[STATUS] Failed to send periodic status", exc_info=True)
                    
                    if cycle_count % 10 == 0:
                        perf_stats = self.performance_monitor.get_stats()
                        logger.info(f"[PERF] Cycle {cycle_count}: {perf_stats['avg_cycle_time']:.2f}s, "
                                  f"{perf_stats['avg_opportunities_per_cycle']:.1f} ops/cycle")
                        # periodic rebalance check (once per 10 cycles)
                        try:
                            if self.config.rebalance_enabled:
                                # ensure only one rebalance task runs at a time
                                task = getattr(self.wallet, '_rebalance_task', None)
                                if not task or task.done():
                                    self.wallet._rebalance_task = asyncio.create_task(self.wallet.rebalance_wallets_async())
                        except Exception as e:
                            logger.debug(f"[REBALANCE] Periodic rebalance failed: {e}", exc_info=True)
                    
                    self._cleanup_cooldowns()
                    
                    elapsed = time.time() - cycle_start_time
                    sleep_time = max(0, self.config.scan_interval - elapsed)
                    await asyncio.sleep(sleep_time)
                    
                except Exception as e:
                    logger.error(f"[ERROR] In main loop cycle {cycle_count}: {e}")
                    await asyncio.sleep(30)
                    
        except KeyboardInterrupt:
            logger.info("[SHUTDOWN] Stopping by user request")
            await self.telegram.send_message("🛑 Бот остановлен по запросу пользователя")
        except Exception as e:
            logger.error(f"[ERROR] Critical error: {e}")
            await self.telegram.send_message(f"🚨 Критическая ошибка: {e}")
        finally:
            self.is_running = False
            self.analytics.export_to_csv("trades_history.csv")
            
    async def close(self):
        self.is_running = False
        
        for client_name, client in self.clients.items():
            await client.close()
        
        # stop virtual reporter
        if self._virtual_report_task:
            self._virtual_report_task.cancel()
            try:
                await self._virtual_report_task
            except asyncio.CancelledError:
                pass

        await self.telegram.close()
        
        logger.info("[SHUTDOWN] Bot resources released")

# ----------------- MAIN -----------------
async def main():
    bot = None
    try:
        bot = ArbitrageEngineV9_1(app_config)
        await bot.initialize()
        await bot.run()
    except KeyboardInterrupt:
        logger.info("🛑 KeyboardInterrupt received. Stopping bot...")
    except Exception as e:
        logger.error(f"[ERROR] Failed to start bot: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        try:
            if bot and hasattr(bot, 'telegram') and bot.telegram:
                await bot.telegram.send_message(f"❌ Ошибка запуска бота: {e}")
        except:
            pass
    finally:
        if bot:
            await bot.close()

if __name__ == "__main__":
    # Compose startup banner but do NOT print it to the terminal.
    startup_msg = (
        "🚀 Arbitrage Bot v9.1 - ENHANCED VERSION\n"
        "💰 Virtual balance: $3000 ($600 per exchange)\n"
        "🔒 TEST MODE ONLY - NO REAL TRADING\n"
        "📊 5 Exchanges: Binance, Bybit, Gate, MEXC, Bitget\n"
        "🎯 Market types: Spot, Futures\n"
        "⚡ Performance-optimized scanning"
    )

    async def _notify_startup(token: str, chat_id: str, message: str):
        """Send a one-off startup message via Telegram and close the notifier.

        This helper uses a temporary TelegramNotifier so the message is delivered
        to the configured chat without printing the banner to the local terminal.
        """
        try:
            tn = TelegramNotifier(token, chat_id, app_config.telegram_min_alert_interval)
            ok = await tn.start()
            if ok:
                # force delivery regardless of throttling logic
                await tn.send_message(message, force=True, priority=10)
            await tn.close()
        except Exception:
            # Do not print on terminal; failures are non-fatal here
            pass

    # Fire-and-forget: notify Telegram synchronously before starting the main loop
    try:
        asyncio.run(_notify_startup(app_config.telegram_token, app_config.telegram_chat_id, startup_msg))
    except Exception:
        # If notification fails, continue silently
        pass

    # Start the bot main loop
    asyncio.run(main())
