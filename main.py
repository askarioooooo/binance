import asyncio
import math
import logging
import ujson
import json
from aiohttp import ClientTimeout, TCPConnector
from binance import AsyncClient, BinanceSocketManager
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
from binance.exceptions import BinanceAPIException
import httpx
import time
import aiohttp
from urllib.parse import urlencode, quote_plus
import hashlib
import hmac
import datetime
import signal
import os
import sys
from collections import defaultdict
from collections import deque
import orjson
from aiohttp import WSMsgType
from logging.handlers import RotatingFileHandler
from typing import Optional
from asyncio import Lock
from aiohttp import web
from dotenv import load_dotenv
import psutil
import asyncio, traceback
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError
from aiohttp import ClientSession
import random
import time
from aiohttp import ClientTimeout, TCPConnector, AsyncResolver
import socket
import subprocess
import ctypes
import subprocess
import sys
import os
import asyncio
import logging
import aiohttp
from bisect import bisect_left, bisect_right
from urllib.parse import quote_plus
from asyncio import create_task
import json
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from asyncio import Queue
import json
from pathlib import Path

BACKUP_FILE = Path("symbol_candles_backup.txt")
load_dotenv("env.txt")

log_file = "debug_trace.log"
max_log_size = 100 * 1024 * 1024  # 100 MB
backup_count = 5
file_handler = RotatingFileHandler(log_file,
                                   maxBytes=max_log_size,
                                   backupCount=backup_count,
                                   encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.addHandler(file_handler)
root_logger.setLevel(logging.DEBUG)


TELEGRAM_TOKEN = '5613945798:AAHgGiTcn1SJr6zlJA66uSCDlC5hXzzQwdw'
TELEGRAM_CHAT_ID = '812257655'

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
if API_SECRET is None:
    raise ValueError("❌ BINANCE_SECRET переменная окружения не установлена")
if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "❌ API_KEY и/или API_SECRET не заданы. Проверь переменные окружения.")

BINANCE_BASE_URL = "https://fapi.binance.com"

LEVERAGE = 30
STOP_LOSS_PCT = 0.0025
TAKE_PROFIT_PCT = 0.03
MIN_CANDLE_BODY_PCT = 3.0


MAX_ORDER_RETRIES = 3
RETRY_DELAY_SEC = 0.1
CANDLE_INTERVAL_MS = 60_000  # 1 минута
WATCHDOG_TIMEOUT = 15  # секунд
RECONNECT_MAX_DELAY = 30
MAX_TRADES_PER_SYMBOL = 5000
CHUNK_SIZE = 35
INACTIVITY_TIMEOUT = 600

logger = logging.getLogger(__name__)

DETACHED_PROCESS = 0x00000008 | 0x00000200  
last_aggtrade_ts = defaultdict(lambda: 0.0)  
aggtrade_stream_tasks = {}
consecutive_dns_failures = 0
ignored_signals_countdown = 0
consecutive_stop_count = 0
time_offset = 0
connection_count = 0
aiohttp_session: aiohttp.ClientSession | None = None
latest_candles = {}
buffer_lock = asyncio.Lock()
current_position_symbol = None
current_position_lock = asyncio.Lock()
active_positions = {}
active_positions_lock = asyncio.Lock()
shutdown_event = asyncio.Event()
symbol_trade_cache = defaultdict(lambda: deque())  
symbol_last_candle_time = {}
usdt_balance = None
usdt_balance_lock: Lock = Lock() 
last_signal_symbol = None
last_force_finalize_ts = time.time()
dns_error_count = 0
MAX_DNS_ERRORS_BEFORE_RESTART = 5
delayed_signals = {}
symbol_candles: dict[str, dict[int, list[float]]] = defaultdict(dict)

aggtrade_queue = deque()

aiohttp_session: aiohttp.ClientSession | None = None

def get_server_time_ms() -> int:
    return int(time.time() * 1000) - time_offset

async def check_offset(session_http):
    try:
        async with session_http.get("https://fapi.binance.com/fapi/v1/time") as resp:
            data = await resp.json(loads=ujson.loads)
            server_time = data["serverTime"] / 1000
            local_time = time.time()
            offset = server_time - local_time
            print(f"⏱ Разница между временем Binance и локальным: {offset:.3f} сек")
    except asyncio.CancelledError:
        logging.info("🛑 check_offset отменён")
    except Exception as e:
        logging.warning(f"⚠️ Ошибка в check_offset: {e}")





async def dns_check(host="fapi.binance.com"):
    try:
        await asyncio.get_event_loop().getaddrinfo(host, 443)
        return True
    except Exception as e:
        logging.error(f"❌ DNS check failed for {host}: {e}")
        return False
    

def sign_request(payload: dict, secret: str) -> str:
    payload_with_ts = payload.copy()
    payload_with_ts["timestamp"] = get_server_time_ms()
    query = urlencode(payload_with_ts)
    signature = hmac.new(secret.encode(), query.encode(),
                         hashlib.sha256).hexdigest()
    full_query = f"{query}&signature={signature}"
    return full_query


def round_down(number, decimals):
    factor = 10**decimals
    return math.floor(number * factor) / factor

def setup_graceful_shutdown(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown()))


async def shutdown():
    logging.info("🔻 Завершение... отмена задач и async генераторов")
    shutdown_event.set()

    current = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks() if t is not current and not t.done()]

    for t in tasks:
        t.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            logging.error(f"❗ Task[{i}] завершился с ошибкой: {result}")

    await asyncio.get_event_loop().shutdown_asyncgens()


async def close_position(session, client, symbol, symbol_meta): 
    global current_position_symbol, usdt_balance

    try:
        await cancel_all_open_orders_and_close_position(session, client, symbol, API_KEY, API_SECRET)
    except Exception as e:
        logging.error(f"❌ Ошибка при отмене ордеров в close_position: {e}")
    finally:
        async with current_position_lock:
            current_position_symbol = None
        async with usdt_balance_lock:
            usdt_balance = await get_usdt_balance(session, client)


async def safe_get_order(session, symbol, order_id, API_KEY, API_SECRET, delay=0.01):
    url = "https://fapi.binance.com/fapi/v1/order"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "orderId": order_id,
        "timestamp": get_server_time_ms(),
        "recvWindow": 2000
    }
    signed_query = sign_request(payload, API_SECRET)

    while True:
        try:
            async with session.get(f"{url}?{signed_query}", headers=headers) as response:
                return await response.json()
        except Exception as e:
            logging.warning(f"⏳ Ошибка get_order {order_id} {symbol}: {e}")
            await asyncio.sleep(delay)


async def safe_get_ticker(session, symbol, API_KEY, API_SECRET, delay=0.01):
    url = "https://fapi.binance.com/fapi/v1/ticker/price"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "timestamp": get_server_time_ms(),
        "recvWindow": 2000
    }
    signed_query = sign_request(payload, API_SECRET)

    while True:
        try:
            async with session.get(f"{url}?{signed_query}", headers=headers) as response:
                return await response.json()
        except Exception as e:
            logging.warning(f"⏳ get_ticker {symbol} ошибка: {e}")
            await asyncio.sleep(delay)


async def safe_create_order(session, symbol_meta, API_KEY, API_SECRET, **order_kwargs) -> Optional[list[dict]]:
    """
    Минимизированная по задержкам версия — меньше логов, меньше проверок, быстрый выход при успехе.
    """
    global usdt_balance, limit_guard_global

    symbol = order_kwargs["symbol"]
    meta = symbol_meta.get(symbol)
    if not meta:
        return None

    qty = round(order_kwargs["quantity"], meta['quantity_precision'])
    if qty <= 0 and not order_kwargs.get("closePosition", False):
        return None

    batch_orders = []
    side = order_kwargs["market_side"]
    pos_side = order_kwargs.get("positionSide", "BOTH")

    if order_kwargs.get("price"):
        batch_orders.append({
            "symbol": symbol, "side": side, "type": "LIMIT",
            "price": str(order_kwargs["price"]), "quantity": str(qty),
            "timeInForce": "GTC", "positionSide": pos_side
        })

    if order_kwargs.get("stopPrice"):
        batch_orders.append({
            "symbol": symbol, "side": "SELL" if side == "BUY" else "BUY",
            "type": "STOP", "stopPrice": str(order_kwargs["stopPrice"]),
            "price": str(order_kwargs["stopPrice"]), "quantity": str(qty),
            "timeInForce": "GTC", "positionSide": pos_side, "reduceOnly": "true"
        })

    if order_kwargs.get("takePrice"):
        batch_orders.append({
            "symbol": symbol, "side": "SELL" if side == "BUY" else "BUY",
            "type": "TAKE_PROFIT", "stopPrice": str(order_kwargs["takePrice"]),
            "price": str(order_kwargs["takePrice"]), "quantity": str(qty),
            "timeInForce": "GTC", "positionSide": pos_side, "reduceOnly": "true"
        })

    if not batch_orders:
        return None

    url = f"{BINANCE_BASE_URL}/fapi/v1/batchOrders"
    headers = {"X-MBX-APIKEY": API_KEY}

    for attempt in range(3):
        payload = {
            "batchOrders": ujson.dumps(batch_orders),
            "timestamp": get_server_time_ms(),
            "recvWindow": 2000
        }
        signature = hmac.new(API_SECRET.encode(), urlencode(payload, quote_via=quote_plus).encode(), hashlib.sha256).hexdigest()
        payload["signature"] = signature

        try:
            async with session.post(url, headers=headers, data=payload) as resp:
                data = await resp.json(loads=ujson.loads)
                if isinstance(data, list):
                    return data
                elif "timestamp for this request was" in data.get("msg", "").lower():
                    await sync_time_once(order_kwargs.get("client"))
        except Exception:
            await asyncio.sleep(0.05)

    limit_guard_global = False
    await cancel_all_open_orders_and_close_position(session, order_kwargs.get("client"), symbol, API_KEY, API_SECRET)
    return None

async def safe_futures_account(session, API_KEY, API_SECRET):
    url = "https://fapi.binance.com/fapi/v2/account"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {"recvWindow": 2000, "timestamp": get_server_time_ms()}
    signed_query = sign_request(payload, API_SECRET)
    full_url = f"{url}?{signed_query}"

    try:
        async with session.get(full_url, headers=headers) as response:
            data = await response.json()
            if not isinstance(data, dict):
                logging.error(f"❌ Неожиданный ответ safe_futures_account: {data}")
                return {}
            if data.get("code") is not None and data.get("code") != 0:
                logging.error(f"❌ Binance API ошибка: {data}")
                return {}
            return data
    except Exception as e:
        logging.error(f"❌ safe_futures_account ошибка: {e}")
        return {}


async def safe_futures_position_info(session, symbol, API_KEY, API_SECRET, delay=0.01):
    url = "https://fapi.binance.com/fapi/v2/positionRisk"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "timestamp": get_server_time_ms(),
        "recvWindow": 3500
    }
    signed_query = sign_request(payload, API_SECRET)

    try:
        while not shutdown_event.is_set():
            try:
                async with session.get(f"{url}?{signed_query}", headers=headers) as response:
                    return await response.json()
            except Exception as e:
                logging.warning(f"⏳ position_info {symbol}: {e}")
                await asyncio.sleep(delay)
    except asyncio.CancelledError:
        logging.info("🛑 safe_futures_position_info отменён")


async def get_usdt_balance(session, client):
    account_info = await safe_futures_account(session, API_KEY, API_SECRET)
    if not account_info or "assets" not in account_info:
        return 0.0

    for asset in account_info["assets"]:
        if asset.get("asset") == "USDT":
            return float(asset.get("availableBalance", 0.0))
    return 0.0

MAX_CONCURRENT_TASKS = 100
active_tasks = set()

usdt_balance_cache = {"value": None, "ts": 0}
semaphore = asyncio.Semaphore(50)


async def cancel_all_open_orders_and_close_position(session, client, symbol, API_KEY, API_SECRET):
    MAX_RETRIES = 5
    RETRY_DELAY = 1.5

    async def cancel_orders():
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                payload = {
                    "symbol": symbol,
                    "timestamp": get_server_time_ms(),
                    "recvWindow": 3000
                }
                signed_query = sign_request(payload, API_SECRET)
                url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{signed_query}"
                headers = {"X-MBX-APIKEY": API_KEY}

                async with session.delete(url, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status == 200:
                        logging.info(f"🚮 Все ордера по {symbol} отменены (попытка {attempt})")
                        return True
                    else:
                        logging.warning(f"⚠️ Не удалось отменить ордера по {symbol} (попытка {attempt}): {resp.status} {text}")
                        if '"code":-1021' in text:
                            logging.warning(f"🔄 Попытка синхронизации времени после -1021")
                            await sync_time_once(client)
            except Exception as e:
                logging.warning(f"⚠️ Ошибка отмены ордеров по {symbol} (попытка {attempt}): {e}")
            await asyncio.sleep(RETRY_DELAY)
        return False

    async def close_position():
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                positions = await client.futures_position_information(symbol=symbol)
                for pos in positions:
                    amt = float(pos["positionAmt"])
                    if amt != 0.0:
                        side = "SELL" if amt > 0 else "BUY"
                        abs_amt = abs(amt)
                        logging.warning(f"⚠️ Позиция по {symbol} не закрыта ({amt}) — закрываем (попытка {attempt})")

                        order = await client.futures_create_order(
                            symbol=symbol,
                            side=side,
                            type="MARKET",
                            quantity=abs_amt,
                            reduceOnly=True,
                            timestamp=get_server_time_ms()
                        )
                        logging.info(f"✅ Позиция по {symbol} закрыта: {order}")
                        return True
                return True  
            except Exception as e:
                logging.warning(f"⚠️ Ошибка закрытия позиции по {symbol} (попытка {attempt}): {e}")
                if '"code":-1021' in str(e):
                    logging.warning(f"🔄 Попытка синхронизации времени после -1021 в close_position()")
                    await sync_time_once(client)

            await asyncio.sleep(RETRY_DELAY)
        return False

    orders_canceled = await cancel_orders()
    position_closed = await close_position()

    if orders_canceled and position_closed:
        logging.info(f"✅ cancel_all_open_orders_and_close_position завершена для {symbol}")
    else:
        logging.error(f"❌ Не удалось полностью очистить {symbol} после {MAX_RETRIES} попыток")



async def monitor_order_updates(session, client, symbol_meta):
    global consecutive_stop_count, ignored_signals_countdown, current_position_symbol
    global usdt_balance, limit_guard_global

    async def send_ping_forever(ws):
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(60)
                try:
                    await ws.ping()
                except Exception as e:
                    logging.warning(f"⚠️ Ошибка при ping: {type(e).__name__} — {e}")
        except asyncio.CancelledError:
            logging.info("😛 send_ping_forever отменён")

    async def renew_listen_key_forever(listen_key, stop_event):
        try:
            while not stop_event.is_set():
                await asyncio.sleep(30 * 60)
                try:
                    await client.futures_stream_keepalive(listen_key)
                    logging.info("🔁 listenKey обновлён")
                except BinanceAPIException as e:
                    if e.code == -1125:
                        logging.error("🚫 listenKey недействителен — сервер отклонил keepalive")
                        stop_event.set()
                        return
                    else:
                        logging.warning(f"⚠️ BinanceAPIException при обновлении listenKey: {e}")
                except Exception as e:
                    logging.warning(f"⚠️ Общая ошибка при обновлении listenKey: {e}")
        except asyncio.CancelledError:
            logging.info("🛑 renew_listen_key отменён")

    while not shutdown_event.is_set():
        try:
            listen_key = await client.futures_stream_get_listen_key()
            if shutdown_event.is_set():
                break

            url = f"wss://fstream.binance.com/ws/{listen_key}"
            logging.info(f"🔄 Подключение к {url} через aiohttp WebSocket...")

            renew_stop_event = asyncio.Event()
            renew_task = asyncio.create_task(renew_listen_key_forever(listen_key, renew_stop_event))

            # Отдельный таймаут для WebSocket, чтобы избежать ConnectionTimeoutError
            ws_timeout = ClientTimeout(connect=10, total=None)
            async with session.ws_connect(url, autoping=False, receive_timeout=None, timeout=ws_timeout) as ws:
                logging.info("✅ User Data WebSocket подключён")
                ping_task = asyncio.create_task(send_ping_forever(ws))
                watchdog_task = asyncio.create_task(ws_watchdog(ws, name="user_data"))

                error_happened = False

                try:
                    async for msg in ws:
                        if shutdown_event.is_set():
                            break

                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                            logging.warning(f"🔒 WebSocket закрыт: {msg.type}")
                            error_happened = True
                            break

                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue

                        try:
                            event = msg.json()
                        except Exception as e:
                            logging.warning(f"⚠️ Ошибка парсинга JSON: {type(e).__name__} — {e}")
                            continue

                        if event.get('e') != 'ORDER_TRADE_UPDATE':
                            continue

                        data = event.get('o', {})
                        symbol = data.get('s')
                        status = data.get('X')
                        order_type = data.get('ot')
                        side = data.get('S')
                        qty = float(data.get('q', 0))
                        price = float(data.get('p', 0))
                        avg_price = float(data.get('ap', 0))
                        stop_price = data.get('sp', 'N/A')
                        reduce_only = data.get('R')
                        close_position = data.get('cp')
                        order_id = data.get('i')
                        client_order_id = data.get('c')
                        ts = data.get('T')

                        logging.info(
                            f"🧾 Обновление ордера: #{order_id} | {symbol} | side={side} | type={order_type} "
                            f"| qty={qty} | price={price} | avgPrice={avg_price} | stopPrice={stop_price} "
                            f"| status={status} | reduceOnly={reduce_only} | closePosition={close_position} "
                            f"| time={ts} | clientOrderId={client_order_id}"
                        )

                        if status == 'FILLED' and order_type == 'LIMIT':
                            limit_guard_global = True

                        elif status == 'FILLED' and order_type in ('STOP', 'TAKE_PROFIT'):
                            limit_guard_global = False
                            logging.warning(f"{symbol}: сработал {order_type} — удаляем ордера")
                            await cancel_all_open_orders_and_close_position(session, client, symbol, API_KEY, API_SECRET)
                            async with current_position_lock:
                                if current_position_symbol == symbol:
                                    current_position_symbol = None
                            if order_type == 'STOP':
                                consecutive_stop_count += 1
                                ignored_signals_countdown = 1
                            async with usdt_balance_lock:
                                usdt_balance = await get_usdt_balance(session, client)
                                logging.info(f"💰 Баланс обновлён: {usdt_balance:.4f} USDT")

                        elif status == 'EXPIRED' and order_type in ('STOP', 'TAKE_PROFIT'):
                            await asyncio.sleep(10)
                            recent_orders = await safe_get_order(session, symbol, order_id, API_KEY, API_SECRET)
                            recent_status = recent_orders.get("status", "UNKNOWN") if isinstance(recent_orders, dict) else "UNKNOWN"
                            if recent_status != "NEW":
                                logging.warning(f"{symbol}: {order_type} НЕ пересоздан — удаляем ордера")
                                limit_guard_global = False
                                await cancel_all_open_orders_and_close_position(session, client, symbol, API_KEY, API_SECRET)
                                async with usdt_balance_lock:
                                    usdt_balance = await get_usdt_balance(session, client)
                                    logging.info(f"💰 Баланс обновлён: {usdt_balance:.4f} USDT")

                except asyncio.CancelledError:
                    logging.info("🛑 monitor_order_updates отменён")
                    break
                except (asyncio.TimeoutError, aiohttp.ClientError, ConnectionResetError) as e:
                    logging.warning(f"📡 WebSocket ошибка: {type(e).__name__} — {e}")
                    error_happened = True
                except Exception as e:
                    logging.error(f"❌ Неизвестная ошибка WebSocket: {type(e).__name__} — {e}")
                    error_happened = True
                finally:
                    ping_task.cancel()
                    watchdog_task.cancel()
                    renew_stop_event.set()
                    renew_task.cancel()
                    await asyncio.gather(ping_task, watchdog_task, renew_task, return_exceptions=True)

                    if error_happened and not ws.closed:
                        await ws.close()
                        logging.warning(f"🧨 WebSocket закрыт вручную из-за ошибки (id={id(ws)})")
                    elif not error_happened:
                        logging.info(f"🔚 WebSocket завершён без ошибок (id={id(ws)})")

        except asyncio.CancelledError:
            logging.info("🛑 monitor_order_updates отменён")
            break
        except Exception as e:
            logging.error(f"❌ Ошибка на верхнем уровне monitor_order_updates: {type(e).__name__} — {e}")
            await asyncio.sleep(5)

        
async def recreate_global_sessions():
    global aiohttp_session_ws, aiohttp_session_http

    # Закрытие старых сессий
    try:
        if aiohttp_session_ws and not aiohttp_session_ws.closed:
            await aiohttp_session_ws.close()
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при закрытии старой WS-сессии: {e}")
    try:
        if aiohttp_session_http and not aiohttp_session_http.closed:
            await aiohttp_session_http.close()
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при закрытии старой HTTP-сессии: {e}")

    # Проверка DNS с 10 попытками
    for attempt in range(1, 11):
        if await dns_check("fapi.binance.com"):
            logging.info(f"✅ DNS разрешён успешно (попытка {attempt})")
            break
        else:
            logging.warning(f"🌐 DNS ошибка (попытка {attempt}/10) — повтор через 5с")
            await asyncio.sleep(5)
    else:
        logging.critical("🚫 DNS check не прошёл за 10 попыток — возможно проблемы на уровне сети")

    resolver = AsyncResolver()

    # Сессия для WebSocket
    ws_timeout = ClientTimeout(total=10, connect=5, sock_read=5, sock_connect=5)
    ws_connector = TCPConnector(
        limit=1000,
        limit_per_host=50,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        keepalive_timeout=15
    )
    aiohttp_session_ws = ClientSession(
        connector=ws_connector,
        timeout=ws_timeout,
        json_serialize=ujson.dumps
    )

    # Сессия для HTTP-запросов
    http_timeout = ClientTimeout(total=10, connect=5, sock_read=5, sock_connect=5)
    http_connector = TCPConnector(
        limit=500,
        limit_per_host=200,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        keepalive_timeout=10,
        resolver=resolver
    )
    aiohttp_session_http = ClientSession(
        connector=http_connector,
        timeout=http_timeout,
        json_serialize=ujson.dumps
    )

    logging.info("✅ Созданы сессии: WebSocket (стабильная) и HTTP (быстрая)")



async def sync_time_with_binance(client):
    global consecutive_dns_failures

    while not shutdown_event.is_set():
        try:
            await client.ping()
            await client.get_server_time()

            if consecutive_dns_failures > 0:
                consecutive_dns_failures = 0
                logging.info("✅ DNS восстановлен")

            return

        except aiohttp.ClientConnectorError as e:
            if "getaddrinfo failed" in str(e):
                consecutive_dns_failures += 1
                logging.warning(f"❌ DNS ошибка #{consecutive_dns_failures}: {e}")
                if consecutive_dns_failures >= MAX_DNS_ERRORS_BEFORE_RESTART:
                    logging.critical("💥 DNS сбой — перезапуск")
                    subprocess.Popen([sys.executable] + sys.argv)
                    os._exit(1)

                await asyncio.sleep(5)
                continue
            logging.warning(f"⚠️ ClientConnectorError: {e}")
        except Exception as e:
            logging.warning(f"⚠️ Ошибка sync_time_with_binance: {e}")

        await asyncio.sleep(5)

async def create_client_with_retry():
    delay = 5
    max_delay = 60

    try:
        while not shutdown_event.is_set():
            try:
                if not await dns_check("fapi.binance.com"):
                    logging.warning(f"🔁 dns_check не прошёл — повтор через {delay}с")
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, max_delay)
                    continue

                client = await AsyncClient.create(API_KEY, API_SECRET)
                await sync_time_with_binance(client)
                return client
            except Exception as e:
                logging.error(f"❌ Ошибка подключения к Binance: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
    except asyncio.CancelledError:
        logging.info("🛑 create_client_with_retry отменён")


async def sync_time_forever(session_http, interval_seconds=60):
    """
    Периодическая синхронизация времени с Binance.
    Работает через глобальную HTTP-сессию, чтобы оживать после reconnection.
    """
    global time_offset, consecutive_dns_failures
    last_error_type = None

    while not shutdown_event.is_set():
        try:
            async with session_http.get("https://fapi.binance.com/fapi/v1/time", timeout=5) as resp:
                data = await resp.json(loads=ujson.loads)
                server_time = data["serverTime"]
                local_time = int(time.time() * 1000)
                time_offset = server_time - local_time

            if consecutive_dns_failures > 0:
                consecutive_dns_failures = 0
                logging.info("✅ DNS восстановлен")

            last_error_type = None

        except aiohttp.ClientConnectorError as e:
            err_type = type(e).__name__
            if err_type != last_error_type:
                logging.warning(f"❌ DNS ошибка: {e}")
                last_error_type = err_type

            if "getaddrinfo failed" in str(e):
                consecutive_dns_failures += 1
                if consecutive_dns_failures >= MAX_DNS_ERRORS_BEFORE_RESTART:
                    logging.critical("💥 DNS сбой — перезапуск")
                    subprocess.Popen([sys.executable] + sys.argv)
                    os._exit(1)

            await asyncio.sleep(5)
            continue

        except asyncio.TimeoutError:
            logging.warning("⏳ Таймаут при запросе времени Binance")
            await asyncio.sleep(3)
            continue

        except Exception as e:
            err_type = type(e).__name__
            if err_type != last_error_type:
                logging.warning(f"⚠️ Ошибка sync_time_forever: {e}")
                last_error_type = err_type

        await asyncio.sleep(interval_seconds)

from datetime import datetime, timezone

last_minute_seen_btc = None





limit_guard_global = False

def round_to_tick(value: Decimal, tick_size: Decimal, direction: str = "down") -> Decimal:
    if direction == "up":
        return (value / tick_size).to_integral_value(rounding=ROUND_UP) * tick_size
    elif direction == "down":
        return (value / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size
    else:
        raise ValueError("direction должен быть 'up' или 'down'")


async def force_finalize_all_minute_candles(client, symbol_meta, API_KEY, API_SECRET, active_positions, active_positions_lock):
    global time_offset, symbol_trade_cache, last_force_finalize_ts
    global usdt_balance, limit_guard_global

    logging.info("🚀 force_finalize (новая стратегия сигнальной свечи) запущена")
    last_processed_minute = None

    try:
        while not shutdown_event.is_set():
            last_force_finalize_ts = time.time()
            now = int(time.time() * 1000)
            current_minute = now - (now % CANDLE_INTERVAL_MS)
            target_minute = current_minute - CANDLE_INTERVAL_MS
            prev_minute_1 = target_minute - CANDLE_INTERVAL_MS
            prev_minute_2 = prev_minute_1 - CANDLE_INTERVAL_MS
            next_minute = current_minute + CANDLE_INTERVAL_MS

            if last_processed_minute == current_minute:
                await asyncio.sleep(0.025)
                continue

            async with usdt_balance_lock:
                local_balance = usdt_balance

            for symbol in list(symbol_candles.keys()):
                candles = symbol_candles.get(symbol, {})
                c_i   = candles.get(target_minute)
                c_i1  = candles.get(prev_minute_1)
                c_i2  = candles.get(prev_minute_2)

                # Нужны три последовательные свечи
                if not c_i or not c_i1 or not c_i2:
                    continue

                o_i,  h_i,  l_i,  cl_i  = map(Decimal, map(str, c_i))
                o_i1, h_i1, l_i1, cl_i1 = map(Decimal, map(str, c_i1))
                o_i2, h_i2, l_i2, cl_i2 = map(Decimal, map(str, c_i2))

                # Цвет свечей
                color_i  = "green" if cl_i > o_i else ("red" if cl_i < o_i else None)
                color_i1 = "green" if cl_i1 > o_i1 else ("red" if cl_i1 < o_i1 else None)
                color_i2 = "green" if cl_i2 > o_i2 else ("red" if cl_i2 < o_i2 else None)
                if not color_i:
                    continue  # дожи — пропуск

                # Тело свечей
                body_i  = abs(cl_i - o_i)  / o_i
                body_i1 = abs(cl_i1 - o_i1) / o_i1
                body_i2 = abs(cl_i2 - o_i2) / o_i2

                # Условие 1: тело свечи i ≥ 3%
                if body_i < Decimal("0.03"):
                    continue

                # Условие 2: i-1 того же цвета и тело меньше чем у i
                if color_i1 != color_i or body_i1 >= body_i:
                    continue

                # Условие 3: i-2 того же цвета и тело меньше чем у i-1
                if color_i2 != color_i or body_i2 >= body_i1:
                    continue

                # Определяем направление сделки
                if color_i == "red":
                    side = SIDE_BUY   # LONG
                    stop_price = cl_i * (Decimal("1.0") - Decimal("0.0025"))  # -0.25%
                    take_price = cl_i * (Decimal("1.0") + Decimal("0.02"))    # +2%
                elif color_i == "green":
                    side = SIDE_SELL  # SHORT
                    stop_price = cl_i * (Decimal("1.0") + Decimal("0.0025"))  # +0.25%
                    take_price = cl_i * (Decimal("1.0") - Decimal("0.02"))    # -2%
                else:
                    continue

                entry_price = cl_i

                if limit_guard_global:
                    logging.info(f"⛔ limit_guard активен — сигнал по {symbol} пропущен")
                    continue

                meta = symbol_meta.get(symbol)
                if not meta:
                    continue
                tick_size = Decimal(str(meta['tick_size']))
                qty_precision = meta['quantity_precision']

                # Округляем цены по направлению
                if side == SIDE_BUY:
                    stop_price  = round_to_tick(stop_price,  tick_size, "up")
                    take_price  = round_to_tick(take_price,  tick_size, "up")
                    entry_price = round_to_tick(entry_price, tick_size, "up")
                else:
                    stop_price  = round_to_tick(stop_price,  tick_size, "down")
                    take_price  = round_to_tick(take_price,  tick_size, "down")
                    entry_price = round_to_tick(entry_price, tick_size, "down")

                qty_usdt = Decimal(str(local_balance)) * Decimal("0.25") * Decimal(str(LEVERAGE))
                qty = round(float(qty_usdt / entry_price), qty_precision)
                if qty <= 0:
                    continue

                asyncio.create_task(
                    safe_create_order(
                        aiohttp_session_http,
                        symbol_meta,
                        API_KEY,
                        API_SECRET,
                        client=client,
                        symbol=symbol,
                        type="BATCH",
                        market_side=side,
                        quantity=qty,
                        price=str(entry_price),
                        stopPrice=str(stop_price),
                        takePrice=str(take_price),
                        positionSide="BOTH"
                    )
                )

                logging.info(
                    f"[SIGNAL] {symbol}: side={side}, qty={qty}, entry={entry_price}, "
                    f"stop={stop_price}, take={take_price}"
                )
                limit_guard_global = True
                break  # один сигнал в минуту

            last_processed_minute = current_minute
            await asyncio.sleep(max(0, (next_minute - int(time.time() * 1000)) / 1000))

    except asyncio.CancelledError:
        logging.info("🛑 force_finalize отменена")


async def watchdog_restart_if_stuck(threshold_seconds: int = 90):
    global last_force_finalize_ts

    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(15)
            now = time.time()
            idle = now - last_force_finalize_ts

            if idle > threshold_seconds:
                logging.critical(f"🧊 force_finalize завис на {idle:.1f} сек — 🔁 перезапуск...")
                await asyncio.sleep(1)
                if not shutdown_event.is_set():
                    subprocess.Popen([sys.executable] + sys.argv)
                    os._exit(1)
                else:
                    logging.info("📴 watchdog_restart_if_stuck: отменено")
    except asyncio.CancelledError:
        logging.info("🛑 watchdog_restart_if_stuck отменён")


async def run_aggtrade_stream(session, client, symbols: list[str], chunk_id: int):
    if shutdown_event.is_set() or not symbols:
        logging.info(f"😛 run_aggtrade_stream[{chunk_id}]: выход (событие завершения или пустой список символов)")
        return

    streams = [f"{s.lower()}@aggTrade" for s in symbols]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    async def send_ping_forever(ws):
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(20)
                try:
                    await ws.ping()
                except Exception as e:
                    logging.warning(f"⚠️ [chunk {chunk_id}] Ошибка при ping: {type(e).__name__} — {e}")
        except asyncio.CancelledError:
            logging.info(f"😛 [chunk {chunk_id}] send_ping_forever отменён")

    async def ws_watchdog(ws):
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(30)
                if ws.closed:
                    logging.warning(f"🕵️‍♂️ [chunk {chunk_id}] watchdog: WebSocket закрыт")
                    return
        except asyncio.CancelledError:
            logging.info(f"😛 [chunk {chunk_id}] ws_watchdog отменён")

    while not shutdown_event.is_set():
        ping_task = None
        watchdog_task = None
        try:
            # 🔹 Ждём пока DNS поднимется
            while not await dns_check("fstream.binance.com"):
                logging.warning(f"🌐 [chunk {chunk_id}] DNS ошибка — ждём 5 секунд...")
                await asyncio.sleep(5)

            async with session.ws_connect(url, autoping=False, receive_timeout=None) as ws:
                logging.info(f"🔗 WebSocket [chunk {chunk_id}] подключён с {len(streams)} stream'ов")

                ping_task = asyncio.create_task(send_ping_forever(ws))
                watchdog_task = asyncio.create_task(ws_watchdog(ws))

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        aggtrade_queue.append(msg.data)
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong(msg.data)
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        logging.warning(f"🔒 [chunk {chunk_id}] WebSocket закрыт: type={msg.type}")
                        break

        except asyncio.CancelledError:
            logging.warning(f"🧨 [chunk {chunk_id}] WebSocket отменён")
            break
        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as e:
            logging.error(f"❌ [chunk {chunk_id}] Ошибка подключения: {type(e).__name__} — {e}")
            await asyncio.sleep(5)
        except Exception as e:
            logging.exception(f"💥 [chunk {chunk_id}] Неизвестная ошибка: {type(e).__name__} — {e}")
            await asyncio.sleep(5)
        finally:
            logging.info(f"📴 [chunk {chunk_id}] WebSocket завершён (в finally)")
            if ping_task:
                ping_task.cancel()
            if watchdog_task:
                watchdog_task.cancel()
            await asyncio.gather(ping_task, watchdog_task, return_exceptions=True)

async def handle_aggtrade():
    try:
        while not shutdown_event.is_set():
            if not aggtrade_queue:  # очередь пустая
                await asyncio.sleep(0.01)
                continue

            raw_msg = aggtrade_queue.popleft()
            try:

                msg = orjson.loads(raw_msg)
                data = msg.get("data", msg)
                symbol = data["s"]
                ts = data["T"]
                price = float(data["p"])

                symbol_trade_cache[symbol].append((ts, price))
                fast_update_minute_candle(symbol, ts, price)

            except Exception as e:
                logging.error(f"[handle_aggtrade] Ошибка обработки трейда: {type(e).__name__}: {e}")

    except asyncio.CancelledError:
        logging.info("🛑 handle_aggtrade отменён")



def fast_update_minute_candle(symbol: str, ts: int, price: float):
    minute_ts = ts - (ts % 60_000)
    candles = symbol_candles[symbol]

    # Удаляем свечи старше 5 минут
    cutoff_ts = minute_ts - 300_000
    old_keys = [k for k in candles.keys() if k < cutoff_ts]
    for k in old_keys:
        del candles[k]

    candle = candles.get(minute_ts)
    if candle is None:
        candles[minute_ts] = [price, price, price, price]  # O, H, L, C
    else:
        o, h, l, _ = candle
        h = max(h, price)
        l = min(l, price)
        c = price
        candles[minute_ts] = [o, h, l, c]

MAX_TRADE_AGE_MS = 120_000  # 2 минуты

async def symbol_trade_cache_cleanup(interval_sec=30):
    try:
        while not shutdown_event.is_set():
            now = int(time.time() * 1000)

            for symbol, dq in list(symbol_trade_cache.items()):
                while dq and dq[0]['ts'] < now - MAX_TRADE_AGE_MS:
                    dq.popleft()

            await asyncio.sleep(interval_sec)
    except asyncio.CancelledError:
        logging.info("🛑 symbol_trade_cache_cleanup отменён")

async def sync_time_once(client):
    global time_offset
    try:
        res = await client.futures_time()
        server_time = res["serverTime"]
        local_time = int(time.time() * 1000) 
        time_offset = server_time - local_time
    except Exception as e:
        print(f"❌ Ошибка sync_time_once: {e}")

async def recv_watchdog(client, all_symbols, threshold_sec=45):
    """
    Следит за активностью символов и перезапускает чанк, если почти все символы в нём молчат.
    """
    global aggtrade_stream_tasks
    REQUIRED_INACTIVE_RATIO = 0.99
    chunk_restart_times = {}
    chunks = list(split_chunks(all_symbols, CHUNK_SIZE))

    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(30)
            now = int(time.time() * 1000)
            inactive_map = {}

            for symbol in all_symbols:
                key = symbol.upper()
                dq = symbol_trade_cache.get(key)
                if not dq or now - dq[-1]['ts'] > threshold_sec * 1000:
                    inactive_map[key] = True

            for name, task in list(aggtrade_stream_tasks.items()):
                if not name.startswith("aggtrade_chunk_"):
                    continue

                try:
                    chunk_index = int(name.split("_")[-1])
                    chunk_symbols = chunks[chunk_index]
                except Exception as e:
                    logging.error(f"❌ Ошибка получения чанка {name}: {e}")
                    continue

                dead_symbols = [s for s in chunk_symbols if s.upper() in inactive_map]
                dead_ratio = len(dead_symbols) / len(chunk_symbols)

                if dead_ratio >= REQUIRED_INACTIVE_RATIO:
                    last_restart = chunk_restart_times.get(chunk_index, 0)
                    if time.time() - last_restart < threshold_sec:
                        continue

                    logging.warning(
                        f"🔁 Перезапуск чанка #{chunk_index}: {len(dead_symbols)}/{len(chunk_symbols)} молчат > {threshold_sec}с"
                    )
                    chunk_restart_times[chunk_index] = time.time()

                    # Остановка старого потока
                    old_task = aggtrade_stream_tasks.get(name)
                    if old_task and not old_task.done():
                        old_task.cancel()
                        try:
                            await old_task
                            logging.info(f"✅ Старый {name} завершён")
                        except asyncio.CancelledError:
                            logging.info(f"⚠️ Старый {name} отменён")
                        except Exception as e:
                            logging.warning(f"⚠️ Завершение старого {name}: {e}")

                    await asyncio.sleep(random.uniform(1.5, 3.0))

                    # Запуск нового потока через стабильную WS-сессию
                    new_task = asyncio.create_task(run_aggtrade_stream(aiohttp_session_ws, client, chunk_symbols, chunk_index))
                    aggtrade_stream_tasks[name] = new_task
                    logging.info(f"🚀 Новый чанк #{chunk_index} запущен ({len(chunk_symbols)} символов)")

    except asyncio.CancelledError:
        logging.info("🛑 recv_watchdog отменён")

import random
import time
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError


async def memory_watchdog(limit_mb=2048):
    try:
        p = psutil.Process()
        while True:
            if shutdown_event.is_set():
                break
            mem_mb = p.memory_info().rss / (1024 * 1024)
            if mem_mb > limit_mb:
                logging.critical(f"💥 memory_watchdog: RSS {mem_mb:.1f}MB — превышен лимит {limit_mb}MB! Перезапуск...")
                subprocess.Popen([sys.executable] + sys.argv)
                os._exit(1)
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        logging.info("🛑 memory_watchdog отменён")

async def aggtrade_watchdog(symbols):
    try:
        while True:
            if shutdown_event.is_set():
                break
            now = time.time()
            for s in symbols:
                key = s.upper()
                last = last_aggtrade_ts.get(key)
                if last is None:
                    continue  # ещё не получен первый трейд
                if now - last > WATCHDOG_TIMEOUT:
                    logging.warning(
                        f"[watchdog] {s}: нет новых трейдов более {WATCHDOG_TIMEOUT}с"
                    )
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        logging.info("🛑 aggtrade_watchdog отменён")



async def ws_watchdog(ws, name="WebSocket"):
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(30)
            if ws.closed:
                logging.warning(f"🛑 {name} закрыт — инициируем рестарт")
                return
    except asyncio.CancelledError:
        return



async def event_loop_debugger():
    while not shutdown_event.is_set():
        await asyncio.sleep(10)
        start = time.perf_counter()
        await asyncio.sleep(0)  # снимаем блокировку
        delay = time.perf_counter() - start
        if delay > 1.5:
            logging.warning(f"⚠️ Event loop завис: задержка {delay:.2f} сек")
            stack = "".join(traceback.format_stack(limit=15))
            logging.warning(f"🧵 Зависание event loop:\n{stack}")


def split_chunks(symbols, size):
    for i in range(0, len(symbols), size):
        yield symbols[i:i + size]


async def launch_all_aggtrade_streams(session, client, symbols: list[str]):
    """
    Запускает все aggTrade-потоки, используя стабильную WS-сессию (aiohttp_session_ws).
    """
    global aggtrade_stream_tasks

    if shutdown_event.is_set():
        logging.info("🛑 shutdown_event установлен — не запускаем aggTrade потоки")
        return []

    chunks = list(split_chunks(symbols, CHUNK_SIZE))
    logging.info(f"🔀 Разбивка на {len(chunks)} чанков по {CHUNK_SIZE} символов")

    # Отмена предыдущих задач
    for name, task in aggtrade_stream_tasks.items():
        if task and not task.done():
            task.cancel()
    aggtrade_stream_tasks.clear()

    total_tokens = 0
    for chunk_id, chunk in enumerate(chunks):
        if shutdown_event.is_set():
            logging.info("🛑 Остановка запуска чанков по флагу завершения")
            break

        task_name = f"aggtrade_chunk_{chunk_id}"
        task = asyncio.create_task(run_aggtrade_stream(aiohttp_session_ws, client, chunk, chunk_id))
        aggtrade_stream_tasks[task_name] = task
        logging.info(f"🚀 Стартует чанк #{chunk_id} — {len(chunk)} токенов")
        total_tokens += len(chunk)

    logging.info(f"✅ Всего активных токенов в WebSocket'ах: {total_tokens}")
    return list(aggtrade_stream_tasks.values())

import aiohttp

logger = logging.getLogger(__name__)

def wait_for_dns(host: str, attempts: int = 10, delay: int = 10) -> None:
    for attempt in range(1, attempts + 1):
        try:
            socket.gethostbyname(host)
            logger.info(f"✅ DNS разрешён успешно: {host}")
            return
        except socket.gaierror:
            logger.warning(f"🌐 DNS ошибка (попытка {attempt}/{attempts}) для {host}")
            time.sleep(delay)
    raise RuntimeError(f"DNS сбой: не удалось разрешить {host} за {attempts} попыток")


async def update_limit_guard_status(session, client): 
    global limit_guard_global

    try:
        # Получаем все открытые ордера
        url_orders = f"{BINANCE_BASE_URL}/fapi/v1/openOrders"
        headers = {"X-MBX-APIKEY": API_KEY}
        params = {"timestamp": get_server_time_ms(), "recvWindow": 2000}
        signed_query = sign_request(params, API_SECRET)

        async with session.get(f"{url_orders}?{signed_query}", headers=headers) as resp:
            orders = await resp.json()
            if not isinstance(orders, list):
                logging.error(f"❌ Неожиданный ответ openOrders: {orders}")
                return

        # Получаем все открытые позиции
        position_info = await safe_futures_position_info(session, '', API_KEY, API_SECRET)
        if not isinstance(position_info, list):
            logging.error(f"❌ Неожиданный ответ position_info: {position_info}")
            return

        position_map = {pos['symbol']: float(pos['positionAmt']) for pos in position_info if float(pos['positionAmt']) != 0.0}

        limit_guard_global = False
        checked_symbols = set()

        for order in orders:
            symbol = order['symbol'].upper()
            if symbol in checked_symbols:
                continue
            checked_symbols.add(symbol)

            symbol_orders = [o for o in orders if o['symbol'].upper() == symbol]
            has_limit = any(o['type'] == 'LIMIT' for o in symbol_orders)
            has_stop = any(o['type'] in ('STOP', 'STOP_MARKET') for o in symbol_orders)
            has_take = any(o['type'] in ('TAKE_PROFIT', 'TAKE_PROFIT_MARKET') for o in symbol_orders)
            has_position = symbol in position_map

            if has_limit and not has_position and has_stop and has_take:
                logging.info(f"✅ limit_guard_global=True по {symbol} (LIMIT + STOP + TP без позиции)")
                limit_guard_global = True
                return

            if not has_limit and has_position and has_stop and has_take:
                logging.info(f"✅ limit_guard_global=True по {symbol} (POSITION + STOP + TP)")
                limit_guard_global = True
                return

            if has_limit and has_position and has_stop and has_take:
                logging.info(f"✅ limit_guard_global=True по {symbol} (LIMIT + POSITION + STOP + TP)")
                limit_guard_global = True
                return

            logging.warning(f"❌ Недопустимая комбинация по {symbol} — отменяем всё")
            await cancel_all_open_orders_and_close_position(session, client, symbol, API_KEY, API_SECRET)

        logging.info("🔻 limit_guard_global=False — все символы очищены")

    except Exception as e:
        logging.error(f"❌ Ошибка в update_limit_guard_status: {e}")
        limit_guard_global = False

def load_symbol_candles():
    """
    Загружает сохранённые свечи из файла при старте.
    """
    if BACKUP_FILE.exists():
        try:
            with BACKUP_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
                for symbol, candles_dict in data.items():
                    symbol_candles[symbol] = {int(k): v for k, v in candles_dict.items()}
            logging.info(f"✅ Загружено {len(symbol_candles)} символов из {BACKUP_FILE}")
        except Exception as e:
            logging.error(f"Ошибка при загрузке свечей из {BACKUP_FILE}: {e}")
    else:
        logging.info("Файл с сохранёнными свечами не найден — старт с пустыми данными.")

def save_symbol_candles():
    """
    Сохраняет все свечи в файл при завершении работы бота.
    """
    try:
        data = {symbol: {str(k): v for k, v in candles.items()} for symbol, candles in symbol_candles.items()}
        with BACKUP_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        logging.info(f"💾 Сохранено {sum(len(v) for v in symbol_candles.values())} свечей в {BACKUP_FILE}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении свечей: {e}")


async def send_symbol_candles_file_to_telegram(interval_minutes=5):
    """
    Каждые interval_minutes минут отправляет TXT-файл с текущими свечами из symbol_candles в Telegram.
    """
    while not shutdown_event.is_set():
        try:
            tmp_file = Path("symbol_candles_snapshot.txt")

            with tmp_file.open("w", encoding="utf-8") as f:
                if not symbol_candles:
                    f.write("Нет данных по свечам.\n")
                else:
                    for symbol, candles in symbol_candles.items():
                        f.write(f"=== {symbol} ===\n")
                        for ts, ohlc in sorted(candles.items()):
                            o, h, l, c = ohlc
                            f.write(f"{ts}|O={o}|H={h}|L={l}|C={c}\n")
                        f.write("\n")

            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"

            form_data = aiohttp.FormData()
            form_data.add_field("chat_id", TELEGRAM_CHAT_ID)
            form_data.add_field("document", tmp_file.open("rb"), filename=tmp_file.name)

            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=form_data) as resp:
                    if resp.status != 200:
                        logging.warning(f"❌ Ошибка отправки symbol_candles в Telegram: {resp.status} {await resp.text()}")
                    else:
                        logging.info("✅ Файл с symbol_candles отправлен в Telegram")

        except Exception as e:
            logging.error(f"❌ Ошибка в send_symbol_candles_file_to_telegram: {e}", exc_info=True)

        await asyncio.sleep(interval_minutes * 60)


async def run_bot_forever():
    while True:
        try:
            wait_for_dns("fapi.binance.com")
            await main()
        except Exception as e:
            logger.error(f"🔥 main() завершился с ошибкой: {e}", exc_info=True)
            logger.info("⏳ Ожидание перед повторным запуском...")
            await asyncio.sleep(10)
        else:
            logger.info("✅ main() завершился без ошибок — выход из цикла")
            break
from aiohttp import web

async def handle(request):
    return web.Response(text="OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
async def main():
    global usdt_balance
    print("🚀 Бот запущен в обновлённой лимитной конфигурации")

    client = None
    tasks = []

    try:
        wait_for_dns("fapi.binance.com")

        client = await create_client_with_retry()
        if client is None:
            logging.critical("❌ create_client_with_retry вернул None — остановка")
            return

        await recreate_global_sessions()
        # ✅ HTTP-сессия для REST-запросов
        session_http = aiohttp_session_http
        # ✅ WebSocket-сессия для стримов
        session_ws = aiohttp_session_ws

        await check_offset(aiohttp_session_http)

        logging.info("✅ Подключение к Binance прошло успешно")

        try:
            futures_balance = await client.futures_account_balance()
            usdt_balance = next((x for x in futures_balance if x['asset'] == 'USDT'), None)
            if usdt_balance:
                logging.info("📊 Баланс фьючерсного аккаунта: %s USDT", usdt_balance['balance'])
            else:
                logging.warning("⚠️ Баланс USDT не найден")
        except Exception as e:
            logging.error(f"❌ Ошибка получения баланса: {e}", exc_info=True)
            raise

        exchange_info = await client.futures_exchange_info()
        symbols = [
            s['symbol'] for s in exchange_info['symbols']
            if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING' and s['symbol'].endswith('USDT')
        ]
        logging.info(f"📈 Получено {len(symbols)} торговых пар")

        symbol_meta = {
            s['symbol']: {
                'tick_size': float(f['tickSize']),
                'price_precision': s['pricePrecision'],
                'quantity_precision': s['quantityPrecision'],
                'min_qty': float(f2['minQty']),
                'min_notional': float(f3['notional']) if f3 else None,
            }
            for s in exchange_info['symbols']
            if s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING' and s['symbol'].endswith('USDT')
            for f in s['filters'] if f['filterType'] == 'PRICE_FILTER'
            for f2 in s['filters'] if f2['filterType'] == 'LOT_SIZE'
            for f3 in (next((x for x in s['filters'] if x['filterType'] == 'MIN_NOTIONAL'), None),)
        }

        await sync_time_once(client)
        usdt_balance = await get_usdt_balance(session_http, client)
        await update_limit_guard_status(session_http, client)
        load_symbol_candles()
        logging.info(f"🔄 Guard статус после запуска: {limit_guard_global}")

        # 🔄 Запускаем все потоки аггрегированных трейдов (через WS-сессию)
        stream_tasks = await launch_all_aggtrade_streams(session_ws, client, symbols)
        logging.info(f"✅ Запущено {len(stream_tasks)} потоков aggTrade по символам")

        tasks = [
            # Для ордеров и REST-запросов используем HTTP-сессию
            asyncio.create_task(force_finalize_all_minute_candles(
                client, symbol_meta, API_KEY, API_SECRET, active_positions, active_positions_lock
            )),

            # Для user data stream используем WebSocket-сессию
            asyncio.create_task(monitor_order_updates(session_ws, client, symbol_meta)),

            asyncio.create_task(sync_time_forever(session_http)),
            asyncio.create_task(watchdog_restart_if_stuck()),
            asyncio.create_task(memory_watchdog(limit_mb=4096)),
            asyncio.create_task(event_loop_debugger()),
            asyncio.create_task(recv_watchdog(session_http, client, symbols)),
            asyncio.create_task(handle_aggtrade()),
            asyncio.create_task(symbol_trade_cache_cleanup(interval_sec=30)),
            asyncio.create_task(send_symbol_candles_file_to_telegram(interval_minutes=5)),
            *stream_tasks
        ]
        
        await shutdown_event.wait()

    except Exception as e:
        if shutdown_event.is_set():
            logging.info("📄 main завершён по shutdown_event — ошибка подавлена")
        else:
            logging.error(f"❌ Ошибка в main: {e}", exc_info=True)
    finally:
        save_symbol_candles()
        logging.info("🪹 Завершение всех задач...")
        for task in tasks:
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)
        except asyncio.TimeoutError:
            logging.warning("⚠️ Принудительное завершение задач после таймаута")

        if client:
            try:
                await client.close_connection()
            except Exception:
                logging.warning("⚠️ Не удалось закрыть клиент")

        try:
            if aiohttp_session_ws and not aiohttp_session_ws.closed:
                await aiohttp_session_ws.close()
            if aiohttp_session_http and not aiohttp_session_http.closed:
                await aiohttp_session_http.close()
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при закрытии aiohttp сессий: {e}")

        logging.info("🔚 main завершён окончательно")





async def auto_restart_every(hours=1, minutes=0):
    total_seconds = hours * 3600 + minutes * 60
    await asyncio.sleep(total_seconds)

    if shutdown_event.is_set():
        logging.info("📴 auto_restart_every: перезапуск отменён из-за shutdown_event")
        return

    logging.info("🔁 Перезапуск бота через subprocess (Windows-safe)")

    try:
        save_symbol_candles()
        subprocess.Popen(
            [sys.executable] + sys.argv,
            creationflags=DETACHED_PROCESS,
            close_fds=True
        )
        await asyncio.sleep(0.2)
        os._exit(0)
    except Exception as e:
        logging.error(f"❌ Ошибка при попытке перезапуска: {e}")

import traceback

def setup_signal_handlers(loop, main_task):
    def shutdown_handler():
        save_symbol_candles()
        print("🛑 Получен сигнал Ctrl+C — принудительное завершение")
        logging.info("🛑 Сигнал завершения (Ctrl+C) — немедленное завершение")
        try:
            main_task.cancel()
        except Exception:
            pass
        try:
            loop.stop()
        except Exception:
            pass
        os._exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_handler)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown_handler())

if __name__ == "__main__":
    import asyncio
    import logging
    import sys
    import os
    import signal
    import traceback

    logging.basicConfig(level=logging.INFO)
    logging.info(f"🚀 PID процесса при старте: {os.getpid()}")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    async def main_with_shutdown():
        global limit_guard_global

        auto_restart_task = asyncio.create_task(auto_restart_every(hours=1))

        try:
          
            await run_bot_forever()
            await start_web_server()
        except asyncio.CancelledError:
            logging.info("🛑 main_with_shutdown: asyncio.CancelledError")
        except KeyboardInterrupt:
            logging.warning("🛑 KeyboardInterrupt — ручное завершение")
        except Exception as e:
            logging.error("🔥 Необработанная ошибка в main_with_shutdown:")
            logging.error(traceback.format_exc())
        finally:
            shutdown_event.set()
            logging.info("🧹 Завершаем фоновые задачи...")

            auto_restart_task.cancel()
            await asyncio.gather(auto_restart_task, return_exceptions=True)

            await shutdown()

            current = asyncio.current_task()
            others = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
            if others:
                logging.warning(f"🧨 Осталось {len(others)} активных задач — отменяем")
                for t in others:
                    t.cancel()
                await asyncio.gather(*others, return_exceptions=True)

            logging.info("👋 Финальный выход из main_with_shutdown")

    try:
        asyncio.run(main_with_shutdown())
    except KeyboardInterrupt:
        print("🛑 Получен Ctrl+C — завершение процесса")
        os._exit(0)

