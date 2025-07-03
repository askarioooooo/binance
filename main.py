import asyncio
import math
import logging
import ujson
from aiohttp import ClientTimeout, TCPConnector
from binance import AsyncClient, BinanceSocketManager
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
from binance.exceptions import BinanceAPIException
import httpx
import time
import aiohttp
from urllib.parse import urlencode
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

# 👇 stdout логгер
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

# 👇 логгер и файл, и stdout
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.addHandler(file_handler)
root_logger.addHandler(stream_handler)
root_logger.setLevel(logging.DEBUG)

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
if API_SECRET is None:
    raise ValueError("❌ BINANCE_SECRET переменная окружения не установлена")
if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "❌ API_KEY и/или API_SECRET не заданы. Проверь переменные окружения.")

BINANCE_BASE_URL = "https://fapi.binance.com"

LEVERAGE = 20
STOP_LOSS_PCT = 0.007
TAKE_PROFIT_PCT = 0.05
BREAKEVEN_TRIGGER = 0.0075
MIN_CANDLE_BODY_PCT = 7.0

ORDER_TYPE_STOP_MARKET = 'STOP_MARKET'
ORDER_TYPE_TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'

MAX_ORDER_RETRIES = 3
RETRY_DELAY_SEC = 0.1
CANDLE_INTERVAL_MS = 60_000  # 1 минута
WATCHDOG_TIMEOUT = 15  # секунд
RECONNECT_MAX_DELAY = 30
MAX_RECV_QUEUE = 2000
MAX_TRADES_PER_SYMBOL = 5000
recv_queue = asyncio.Queue(maxsize=MAX_RECV_QUEUE)

latest_candles_queue = asyncio.Queue(maxsize=5000)
websocket_messages_queue = asyncio.Queue(maxsize=5000)

last_aggtrade_ts = defaultdict(
    lambda: 0.0)  # теперь значение по умолчанию — float
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
symbol_trade_cache = defaultdict(list)
symbol_last_candle_time = {}
usdt_balance = None
usdt_balance_lock: Lock = Lock()  # 🔒 глобальная блокировка для доступа к балансу
last_signal_symbol = None
last_signal_timestamps: dict[str, float] = {}
SIGNAL_COOLDOWN_SEC = 7200  # 2 часа
last_tick_received = time.time()

aiohttp_session: aiohttp.ClientSession | None = None


async def check_offset():
    client = await AsyncClient.create()
    server_time = (await client.futures_time())['serverTime'] / 1000
    local_time = time.time()
    offset = server_time - local_time
    print(f"⏱ Разница между временем Binance и локальным: {offset:.3f} сек")
    await client.close_connection()


asyncio.run(check_offset())


async def create_global_session():
    global aiohttp_session
    if aiohttp_session is None or aiohttp_session.closed:
        timeout = ClientTimeout(total=2)
        connector = TCPConnector(limit=100,
                                 enable_cleanup_closed=True,
                                 keepalive_timeout=60)
        aiohttp_session = aiohttp.ClientSession(connector=connector,
                                                timeout=timeout,
                                                json_serialize=ujson.dumps)


def sign_request(payload: dict, secret: str) -> str:
    global time_offset
    ts = int(time.time() * 1000) + time_offset
    payload_with_ts = payload.copy()
    payload_with_ts["timestamp"] = ts
    query = urlencode(payload_with_ts)
    signature = hmac.new(secret.encode(), query.encode(),
                         hashlib.sha256).hexdigest()
    full_query = f"{query}&signature={signature}"
    return full_query


def round_down(number, decimals):
    factor = 10**decimals
    return math.floor(number * factor) / factor


async def close_global_session():
    global aiohttp_session
    if aiohttp_session:
        try:
            await aiohttp_session.close()
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при закрытии сессии: {e}")
        aiohttp_session = None


def setup_graceful_shutdown(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown()))


async def shutdown():
    logging.info("🔻 Завершение... закрытие сессии и клиента")
    shutdown_event.set()
    await close_global_session()


async def cancel_stop_orders(client, symbol):
    while True:
        try:
            open_orders = await client.futures_get_open_orders(symbol=symbol)
            stop_orders = [
                o for o in open_orders
                if o['type'] in ['STOP_MARKET', 'STOP_LOSS_LIMIT']
            ]
            for order in stop_orders:
                await safe_cancel_order(client, symbol, order['orderId'])
            if not stop_orders:
                break
        except Exception as e:
            logging.warning(f"{symbol}: Ошибка при отмене стоп-ордеров: {e}")
        await asyncio.sleep(0.01)


async def close_position(client, symbol, symbol_meta):
    await ensure_session()

    global current_position_symbol, usdt_balance
    try:
        positions = await safe_futures_position_info(client, symbol)
        position = next((p for p in positions if float(p['positionAmt']) != 0),
                        None)

        if not position:
            return

        pos_amt = abs(float(position['positionAmt']))
        close_side = SIDE_SELL if float(
            position['positionAmt']) > 0 else SIDE_BUY

        await safe_create_order(symbol_meta,
                                API_KEY,
                                API_SECRET,
                                symbol=symbol,
                                side=close_side,
                                type=ORDER_TYPE_MARKET,
                                quantity=pos_amt,
                                positionSide='BOTH')

    except Exception as e:
        logging.error(f"close_position error: {e}")
    finally:
        async with current_position_lock:
            current_position_symbol = None
        async with usdt_balance_lock:
            usdt_balance = await get_usdt_balance(client)


async def safe_get_order(client, symbol, order_id, delay=0.01):
    await ensure_session()
    if aiohttp_session is None:
        raise RuntimeError(
            "❌ aiohttp_session всё ещё None после ensure_session() в safe_get_order"
        )
    url = "https://fapi.binance.com/fapi/v1/order"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "orderId": order_id,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 2000
    }
    signed_query = sign_request(payload, API_SECRET)

    while True:
        try:
            async with aiohttp_session.get(f"{url}?{signed_query}",
                                           headers=headers) as response:
                return await response.json()
        except Exception as e:
            logging.warning(
                f"⏳ Ошибка при запросе ордера {order_id} по {symbol}, повтор через {delay}с: {e}"
            )
            await asyncio.sleep(delay)


async def safe_get_ticker(client, symbol, delay=0.01):
    await create_global_session()
    if aiohttp_session is None:
        raise RuntimeError(
            "❌ aiohttp_session всё ещё None после ensure_session() в safe_get_ticker"
        )
    url = "https://fapi.binance.com/fapi/v1/ticker/price"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 2000
    }
    signed_query = sign_request(payload, API_SECRET)

    while True:
        try:
            async with aiohttp_session.get(f"{url}?{signed_query}",
                                           headers=headers) as response:
                return await response.json()
        except Exception as e:
            logging.warning(f"⏳ get_ticker ошибка, повтор через {delay}с: {e}")
            await asyncio.sleep(delay)


def round_to_tick(value: float, tick_size: float) -> float:
    precision = len(str(tick_size).split('.')[-1])
    return round((value // tick_size) * tick_size, precision)


async def ensure_session():
    global aiohttp_session
    if aiohttp_session is None or aiohttp_session.closed:
        logging.warning(
            "🔄 aiohttp_session is None or closed. Recreating session...")
        await create_global_session()


async def safe_create_order(symbol_meta, API_KEY, API_SECRET,
                            **params) -> Optional[dict]:
    await ensure_session()
    if aiohttp_session is None:
        raise RuntimeError(
            "❌ aiohttp_session всё ещё None после ensure_session")

    url = "https://fapi.binance.com/fapi/v1/order"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        **params, "timestamp": int(time.time() * 1000),
        "recvWindow": 2000
    }

    signed_query = sign_request(payload, API_SECRET)

    for attempt in range(1, MAX_ORDER_RETRIES + 1):
        try:
            async with aiohttp_session.post(f"{url}?{signed_query}",
                                            headers=headers) as resp:
                data = await resp.json()
                if "orderId" in data:
                    return data
                logging.warning(f"⚠️ Binance ответ без orderId: {data}")
        except Exception as e:
            logging.warning(
                f"❌ Ошибка при создании ордера {params.get('symbol')} попытка {attempt}: {e}"
            )
        await asyncio.sleep(RETRY_DELAY_SEC)

    logging.error(
        f"🛑 Не удалось создать ордер для {params.get('symbol')} после {MAX_ORDER_RETRIES} попыток"
    )
    return {
        "orderId": None
    }  # или можно return None и проверять в вызывающем коде


async def send_order_attempt(url, headers, symbol, order_type, quantity,
                             attempt):
    await ensure_session()
    if aiohttp_session is None:
        raise RuntimeError("❌ aiohttp_session is None после ensure_session()")

    try:
        async with aiohttp_session.post(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=1)) as response:
            res = await response.json()

            if "code" in res and res["code"] < 0:
                logging.warning(
                    f"🛑 Попытка {attempt + 1}: Binance отказал ({res['code']}) в {order_type} по {symbol}: {res}"
                )
                return None

            logging.info(
                f"✅ Попытка {attempt + 1}: Ордер {order_type} по {symbol} (qty: {quantity}) успешно создан"
            )
            return res

    except Exception as e:
        logging.warning(
            f"❗ Попытка {attempt + 1}: Ошибка при создании ордера {order_type} по {symbol}: {str(e)}"
        )
        return None


async def safe_futures_account(client):
    await create_global_session()
    if aiohttp_session is None:
        raise RuntimeError(
            "❌ aiohttp_session всё ещё None после ensure_session() в safe_futures_account"
        )
    url = "https://fapi.binance.com/fapi/v2/account"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {"recvWindow": 2000}

    signed_query = sign_request(payload, API_SECRET)
    full_url = f"{url}?{signed_query}"  # ⬅️ ЭТОЙ строки не хватало

    try:
        async with aiohttp_session.get(full_url, headers=headers) as response:
            res = await response.json()
            return res
    except Exception as e:
        print(f"❌ Ошибка safe_futures_account: {e}")
        return {}


async def safe_futures_position_info(client, symbol, delay=0.01):
    await create_global_session()
    if aiohttp_session is None:
        raise RuntimeError(
            "❌ aiohttp_session всё ещё None после ensure_session() в safe_futures_position_info"
        )
    url = "https://fapi.binance.com/fapi/v2/positionRisk"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 2000
    }
    signed_query = sign_request(payload, API_SECRET)

    while True:
        try:
            async with aiohttp_session.get(f"{url}?{signed_query}",
                                           headers=headers) as response:
                return await response.json()
        except Exception as e:
            logging.warning(
                f"⏳ futures_position_information ошибка, повтор через {delay}с: {e}"
            )
            await asyncio.sleep(delay)


async def get_usdt_balance(client):
    await ensure_session()

    account_info = await safe_futures_account(client)
    if not account_info or "assets" not in account_info:
        return 0.0

    for asset in account_info["assets"]:
        if asset.get("asset") == "USDT":
            return float(asset.get("availableBalance", 0.0))
    return 0.0


async def safe_cancel_order(client, symbol, order_id, delay=0.01):
    await ensure_session()
    if aiohttp_session is None:
        raise RuntimeError(
            "❌ aiohttp_session всё ещё None после ensure_session() в safe_cancel_order"
        )
    url = "https://fapi.binance.com/fapi/v1/order"
    headers = {"X-MBX-APIKEY": API_KEY}
    payload = {
        "symbol": symbol,
        "orderId": order_id,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 2000
    }
    signed_query = sign_request(payload, API_SECRET)

    while True:
        try:
            async with aiohttp_session.delete(f"{url}?{signed_query}",
                                              headers=headers) as response:
                res = await response.json()
                if res.get("code") == -2011:
                    return
                return res
        except Exception as e:
            logging.warning(
                f"{symbol}: ошибка при отмене ордера {order_id}: {e}")
            await asyncio.sleep(delay)


async def process_candle(client, symbol, candle, symbol_meta, usdt_balance,
                         price_at_candle_close, API_KEY, API_SECRET,
                         active_positions, active_positions_lock):
    global current_position_symbol, last_signal_timestamps , last_signal_symbol
    print(
        f"⏱ {datetime.datetime.now().strftime('%H:%M:%S.%f')} process_candle вызван для {symbol}"
    )

    now = time.time()
    last_ts = last_signal_timestamps.get(symbol, 0)
    if last_signal_symbol == symbol:
        logging.info(f"🔁 Пропущен сигнал {symbol} — предыдущий сигнал был по той же паре")
        return
      

    async with current_position_lock:
        if current_position_symbol is not None:
            return

    async with active_positions_lock:
        if symbol in active_positions:
            return

    o, c = float(candle['o']), float(candle['c'])

    side = SIDE_SELL if c > o else SIDE_BUY
    entry_price = c
    meta = symbol_meta[symbol]
    precision = meta['price_precision']
    qty_precision = meta['quantity_precision']

    qty_usdt = usdt_balance * 0.8 * 20
    quantity = round(qty_usdt / price_at_candle_close, qty_precision)

    if quantity <= 0:
        return

    stop_price = entry_price * (1 + 0.007 if side == SIDE_SELL else 1 - 0.007)
    take_price = entry_price * (1 - 0.05 if side == SIDE_SELL else 1 + 0.05)

    send_time = time.time()

    try:
        tasks = [
            safe_create_order(symbol_meta,
                              API_KEY,
                              API_SECRET,
                              symbol=symbol,
                              side=SIDE_SELL if side == SIDE_BUY else SIDE_BUY,
                              type='STOP_MARKET',
                              stopPrice=f"{stop_price:.{precision}f}",
                              closePosition=True,
                              positionSide='BOTH'),
            safe_create_order(symbol_meta,
                              API_KEY,
                              API_SECRET,
                              symbol=symbol,
                              side=side,
                              type='MARKET',
                              quantity=quantity,
                              positionSide='BOTH'),
            safe_create_order(symbol_meta,
                              API_KEY,
                              API_SECRET,
                              symbol=symbol,
                              side=SIDE_SELL if side == SIDE_BUY else SIDE_BUY,
                              type='TAKE_PROFIT_MARKET',
                              stopPrice=f"{take_price:.{precision}f}",
                              closePosition=True,
                              positionSide='BOTH')
        ]

        stop_order, market_order, take_order = await asyncio.gather(*tasks)
        if not market_order or 'orderId' not in market_order:
            return

        server_time = market_order.get('transactTime') or market_order.get(
            'serverTime')
        if server_time:
            server_time_sec = server_time / 1000
            logging.info(
                f"✅ MARKET ордер принят Binance в {server_time_sec:.3f} сек")
            delay = server_time_sec - send_time
            logging.info(f"⏳ Задержка MARKET ордера: {delay:.3f} сек")
        else:
            logging.info("⚠️ В ответе Binance нет времени сервера")

        async with active_positions_lock:
            active_positions[symbol] = {
                'side': side,
                'entry_price': entry_price,
                'price_precision': precision,
                'limit_order_id': None,
                'stop_order_id': stop_order['orderId'] if stop_order else None,
                'take_order_id': take_order['orderId'] if take_order else None,
                'stop_price': stop_price,
                'take_price': take_price
            }

        async with current_position_lock:
            current_position_symbol = symbol

        last_signal_timestamps[symbol] = time.time()
        last_signal_symbol = symbol


    except Exception as e:
        logging.error(f"❌ Ошибка при создании MARKET ордера: {e}")


async def centralized_watch_price(client, bm, symbol_meta, max_chunks=8):

    while True:
        async with active_positions_lock:
            symbols = list(active_positions.keys())

        if not symbols:
            await asyncio.sleep(0.05)
            continue

        chunk_size = max(1, len(symbols) // max_chunks)
        chunks = [
            symbols[i:i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]
        tasks = [
            asyncio.create_task(
                resilient_watch_chunk(client, bm, chunk, symbol_meta))
            for chunk in chunks
        ]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.5)


async def resilient_watch_chunk(client, bm, chunk_symbols, symbol_meta):
    while True:
        try:
            await watch_chunk(client, bm, chunk_symbols, symbol_meta)
        except Exception as e:
            logging.error(f"watch_chunk reconnect: {e}")
            await asyncio.sleep(5)


async def watch_chunk(client, bm, chunk_symbols, symbol_meta):
    global current_position_symbol
    try:
        socket = bm.futures_multiplex_socket(
            [f"{s.lower()}@markPrice" for s in chunk_symbols])
        async with socket as ws_socket:
            ping_task = asyncio.create_task(
                send_ping_forever(ws_socket, interval=60))
            try:
                while True:
                    try:
                        msg = await asyncio.wait_for(ws_socket.recv_json(),
                                                     timeout=15)
                    except asyncio.TimeoutError:
                        logging.warning(
                            "⚠️ watch_chunk: recv_json таймаут — инициируем перезапуск"
                        )
                        raise Exception(
                            "watch_chunk: WebSocket receive timeout")

                    data = msg.get('data', {})
                    symbol = data.get('s')
                    price_str = data.get('p')
                    if not symbol or price_str is None:
                        continue

                    price = float(price_str)
                    async with active_positions_lock:
                        position = active_positions.get(symbol)
                    if not position:
                        continue

                    side = position['side']
                    entry_price = position['entry_price']
                    meta = symbol_meta[symbol]
                    price_precision = meta['price_precision']

                    stop_price = round(
                        entry_price *
                        (1 - STOP_LOSS_PCT if side == SIDE_BUY else 1 +
                         STOP_LOSS_PCT), price_precision)
                    take_price = round(
                        entry_price *
                        (1 + TAKE_PROFIT_PCT if side == SIDE_BUY else 1 -
                         TAKE_PROFIT_PCT), price_precision)

                    triggered = (
                        (side == SIDE_BUY and
                         (price >= take_price or price <= stop_price))
                        or (side == SIDE_SELL and
                            (price <= take_price or price >= stop_price)))

                    if triggered:
                        await close_position(client, symbol, symbol_meta)
                        async with active_positions_lock:
                            active_positions.pop(symbol, None)
                        async with usdt_balance_lock:
                            usdt_balance = await get_usdt_balance(client)
                        async with current_position_lock:
                            if current_position_symbol == symbol:
                                current_position_symbol = None
            finally:
                ping_task.cancel()
                await asyncio.gather(ping_task, return_exceptions=True)
    except Exception as e:
        logging.error(f"watch_chunk error: {e}")


MAX_CONCURRENT_TASKS = 100
active_tasks = set()

usdt_balance_cache = {"value": None, "ts": 0}
semaphore = asyncio.Semaphore(50)


async def send_ping_forever(ws, interval=60):
    while True:
        try:
            await ws.ping()
            await ws.pong()
        except asyncio.CancelledError:
            logging.info("🛑 send_ping_forever отменён")
            raise
        except Exception as e:
            logging.warning(f"⚠️ Ошибка отправки ping/pong: {e}")
        await asyncio.sleep(interval)


async def monitor_order_updates(client, symbol_meta):

    async def renew_listen_key(listen_key):
        while True:
            await ensure_session()
            try:
                await client.futures_stream_keepalive(listen_key)
                logging.info("🔁 listenKey обновлён")
            except Exception as e:
                logging.warning(f"⚠️ Ошибка обновления listenKey: {e}")
            await asyncio.sleep(50 * 60)

    while True:
        try:
            logging.info("🔄 Получение listenKey...")
            listen_key = await client.futures_stream_get_listen_key()
            url = f"wss://fstream.binance.com/ws/{listen_key}"
            logging.info(f"🔄 Подключение к {url} через aiohttp WebSocket...")

            asyncio.create_task(renew_listen_key(listen_key))

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    logging.info("✅ User data WebSocket подключён")
                    ping_task = asyncio.create_task(
                        send_ping_forever(ws, interval=60))
                    watchdog = asyncio.create_task(
                        ws_watchdog(ws, name="user_data"))

                    try:
                        async for msg in ws:
                            if msg.type.name != "TEXT":
                                continue
                            try:
                                event = msg.json()
                            except Exception as e:
                                logging.warning(
                                    f"⚠️ Ошибка парсинга JSON: {e}")
                                continue

                            if event.get('e') == 'ORDER_TRADE_UPDATE':
                                logging.info(
                                    f"monitor_order_updates: событие ORDER_TRADE_UPDATE: {event}"
                                )
                                data = event.get('o', {})
                                symbol = data.get('s')
                                status = data.get('X')
                                order_id = data.get('i')
                                logging.info(
                                    f"📬 Обновление ордера: {symbol} — статус: {status}, ID: {order_id}"
                                )
                    finally:
                        ping_task.cancel()
                        watchdog.cancel()
                        await asyncio.gather(ping_task,
                                             watchdog,
                                             return_exceptions=True)

        except Exception as e:
            logging.error(f"❌ Ошибка в user data WebSocket: {e}")
            logging.info("🔝 Переподключение через 5 секунд...")
            await asyncio.sleep(5)


async def websocket_reader(socket, websocket_messages_queue):
    while True:
        try:
            msg = await socket.recv()
        except Exception as e:
            logging.error(f"WebSocket ошибка чтения: {e}")
            raise  # ⬅️ обязательно пробрасываем ошибку вверх

        if not msg:
            continue

        if msg.get('e') == 'error' and msg.get(
                'type') == 'BinanceWebsocketQueueOverflow':
            logging.error(
                "⚠️ Переполнение WebSocket очереди — инициируем переподключение"
            )
            raise Exception("BinanceWebsocketQueueOverflow")

        await websocket_messages_queue.put(msg)

        if websocket_messages_queue.full():
            logging.warning(
                "⚠️ WebSocket очередь переполнена, инициируем переподключение")
            raise Exception("WebSocket message queue full")


async def sync_active_positions(client):
    while True:
        try:
            await ensure_session()
            global current_position_symbol

            while True:
                try:
                    positions_info = await client.futures_account()
                    if not positions_info or 'positions' not in positions_info:
                        raise ValueError(
                            "Нет ключа 'positions' в ответе Binance")
                    break
                except Exception as e:
                    await asyncio.sleep(1)

            actual_positions = {
                p['symbol']: float(p['positionAmt'])
                for p in positions_info['positions']
                if float(p['positionAmt']) != 0.0
            }

            async with active_positions_lock:
                for symbol in list(active_positions):
                    pos = active_positions[symbol]
                    if symbol not in actual_positions:
                        logging.info(
                            f"{symbol}: позиция закрыта, удаляем из active_positions"
                        )
                        active_positions.pop(symbol, None)

                        async with current_position_lock:
                            if current_position_symbol == symbol:
                                current_position_symbol = None
                                logging.info(
                                    f"{symbol} был текущей позицией, current_position_symbol сброшен"
                                )
                                async with usdt_balance_lock:
                                    usdt_balance = await get_usdt_balance(
                                        client)

        except Exception as e:
            logging.exception("❌ sync_active_positions: критическая ошибка")
        await asyncio.sleep(10)


async def sync_time_with_binance(client):
    while True:
        try:
            await client.ping()
            await client.get_server_time()
            return
        except Exception as e:
            logging.warning(f"⚠️ Ошибка sync_time_forever: {e}")
            await asyncio.sleep(5)


async def create_client_with_retry():
    while True:
        try:
            client = await AsyncClient.create(API_KEY, API_SECRET)
            await sync_time_with_binance(client)
            return client
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к Binance: {e}")
            await asyncio.sleep(5)


async def sync_time_forever(client, interval_seconds=60):
    global time_offset
    while True:
        try:
            res = await client.futures_time()
            server_time = res["serverTime"]
            local_time = int(time.time() * 1000)
            time_offset = server_time - local_time
        except Exception as e:
            logging.warning(f"⚠️ Ошибка sync_time_forever: {e}")
        await asyncio.sleep(interval_seconds)


aggtrade_buckets = defaultdict(list)
last_candle_ts = {}


async def handle_aggtrade(symbol: str, data: dict):
    try:
        ts = int(data['T'])  # Ensure timestamp is in milliseconds
        price = float(data['p'])
        qty = float(data['q'])

        last_aggtrade_ts[symbol] = time.time()
        trade = {'ts': ts, 'price': price, 'qty': qty}

        symbol_trade_cache[symbol].append(trade)

        # Keep only trades for the last 2 minutes
        current_time = int(time.time() * 1000)
        symbol_trade_cache[symbol] = [
            t for t in symbol_trade_cache[symbol]
            if t['ts'] >= current_time - 2 * 60_000
        ]

    except Exception as e:
        logging.warning(f"[aggTrade] Ошибка обработки трейда {symbol}: {e}")


async def ws_reader(ws):
    global last_tick_received

    while True:
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=15)
        except asyncio.TimeoutError:
            logging.warning("⚠️ ws_reader: таймаут — инициируем рестарт")
            raise Exception("WebSocket receive timeout")

        last_tick_received = time.time()

        if msg.type == WSMsgType.PING:
            await ws.pong(msg.data)
            logging.debug("🎾 Pong sent in response to PING")
            continue

        if msg.type != WSMsgType.TEXT:
            continue

        if recv_queue.full():
            logging.warning(
                "⚠️ Очередь recv_queue переполнена — инициируем рестарт")
            raise Exception("recv_queue full")

        await recv_queue.put(msg.data)


async def ws_handler():
    while True:
        try:
            raw = await asyncio.wait_for(recv_queue.get(), timeout=15)
            try:
                payload = orjson.loads(raw)
                symbol = payload['data']['s']
                await handle_aggtrade(symbol, payload['data'])
            except Exception as e:
                logging.warning(f"⚠️ Ошибка парсинга JSON в ws_handler: {e}")
        except asyncio.TimeoutError:
            logging.warning("⚠️ ws_handler: recv_queue.get() завис — рестарт")
            raise Exception("recv_queue.get() timeout")
        except Exception as e:
            logging.warning(f"ws_handler error: {e}")


async def aggtrade_router(client, symbols):
    stream = [f"{s.lower()}@aggTrade" for s in symbols]
    socket = client.futures_multiplex_socket(stream)
    async with socket as ws:
        async for msg in ws:
            data = msg["data"]
            symbol = data["s"]
            await handle_aggtrade(symbol, data)


async def force_finalize_all_minute_candles(client, symbol_meta, API_KEY,
                                            API_SECRET, active_positions,
                                            active_positions_lock,
                                            usdt_balance):
    global time_offset, symbol_trade_cache
    logging.info("🚀 force_finalize_all_minute_candles запущена")

    SYNC_PRECISION = 50
    CANDLE_INTERVAL_MS = 60_000
    SYMBOL_TIMEOUT_MS = 2 * CANDLE_INTERVAL_MS  # 2 минуты без трейдов

    while not shutdown_event.is_set():
        try:
            now = int(time.time() * 1000)
            current_minute = now - (now % CANDLE_INTERVAL_MS)
            next_minute = current_minute + CANDLE_INTERVAL_MS
            trigger_time = current_minute + 59_500

            while now < trigger_time - SYNC_PRECISION:
                await asyncio.sleep((trigger_time - now - SYNC_PRECISION) / 1000)
                now = int(time.time() * 1000)

            process_start = time.time() * 1000
            processed_symbols = 0
            removed_symbols = 0

            for symbol, trades in list(symbol_trade_cache.items()):
                try:
                    if not trades:
                        continue  # не удаляем, ждём до timeout

                    candle_end_time = current_minute + 59_500
                    minute_trades = [
                        t for t in trades
                        if current_minute <= int(t['ts']) < candle_end_time
                    ]

                    if not minute_trades:
                        last_ts = trades[-1]['ts'] if trades else 0
                        if now - last_ts > SYMBOL_TIMEOUT_MS:
                            logging.warning(f"🕑 Удаление {symbol} — нет трейдов более 2 минут")
                            symbol_trade_cache.pop(symbol, None)
                            symbol_last_candle_time.pop(symbol, None)
                            removed_symbols += 1
                        continue

                    prices = [t['price'] for t in minute_trades]
                    candle = {
                        'o': prices[0],
                        'h': max(prices),
                        'l': min(prices),
                        'c': prices[-1],
                        'v': sum(t['qty'] for t in minute_trades),
                        'x': True,
                        't': current_minute
                    }

                    o, c = float(candle['o']), float(candle['c'])

                    if ((o - c) / o) * 100 >= 1:
                        logging.info(f"🕯️ {symbol} raw candle: o={o}, c={c}, diff={((o - c) / o) * 100:.2f}%")
                    if o > c and ((o - c) / o) * 100 >= MIN_CANDLE_BODY_PCT:
                        logging.info(f"[{symbol}] 🔻 Красная свеча -{((o - c) / o) * 100:.2f}%")
                        try:
                            await process_candle(client, symbol, candle,
                                                 symbol_meta, usdt_balance, c,
                                                 API_KEY, API_SECRET,
                                                 active_positions,
                                                 active_positions_lock)
                            processed_symbols += 1
                        except Exception as e:
                            logging.error(f"❌ Ошибка обработки {symbol}: {str(e)}", exc_info=True)

                    symbol_trade_cache.pop(symbol, None)
                    symbol_last_candle_time.pop(symbol, None)
                    removed_symbols += 1

                except Exception as e:
                    logging.error(f"❌ Ошибка в символе {symbol}: {e}", exc_info=True)

            processing_time = time.time() * 1000 - process_start
            logging.info(f"⏱ Обработка {processed_symbols} сигналов заняла {processing_time:.1f}ms")

            now = int(time.time() * 1000) + time_offset
            remaining_time = (next_minute - now) / 1000
            if remaining_time > 0:
                await asyncio.sleep(remaining_time)

        except Exception as e:
            logging.error(f"🔴 Критическая ошибка в force_finalize_all_minute_candles: {str(e)}", exc_info=True)
            await asyncio.sleep(1)

async def run_aggtrade_stream(client, symbols):
    streams = [f"{s.lower()}@aggTrade" for s in symbols]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    await ensure_session()
    if aiohttp_session is None:
        raise RuntimeError("aiohttp_session is None after ensure_session")
    async with aiohttp_session.ws_connect(url) as ws:
        logging.info("🔗 aggTrade поток запущен")
        ping_task = asyncio.create_task(send_ping_forever(ws, interval=60))

        try:
            async for msg in ws:
                if msg.type.name == "TEXT":
                    try:
                        json_msg = msg.json()
                        symbol = json_msg['data']['s']
                        await handle_aggtrade(symbol, json_msg['data'])
                    except Exception as e:
                        logging.warning(
                            f"⚠️ Ошибка чтения сообщения WebSocket: {e}")
        finally:
            ping_task.cancel()
            await asyncio.gather(ping_task, return_exceptions=True)


def split_symbols(symbols, num_groups):
    avg = len(symbols) // num_groups
    return [symbols[i * avg:(i + 1) * avg] for i in range(num_groups - 1)
            ] + [symbols[(num_groups - 1) * avg:]]


async def cleanup_trade_cache():
    while True:
        now = int(time.time() * 1000)
        for sym, sym_trades in list(symbol_trade_cache.items()):
            if sym_trades and (now - sym_trades[-1]['ts']) > (
                    2 * CANDLE_INTERVAL_MS):
                symbol_trade_cache[sym].clear()
        for sym in list(symbol_trade_cache):
            if not symbol_trade_cache[sym]:
                symbol_trade_cache.pop(sym)
                symbol_last_candle_time.pop(sym, None)
        await asyncio.sleep(5)


last_processed_minute = None
minute_stats = defaultdict(list)
minute_stats_lock = asyncio.Lock()


async def sync_time_once(client):
    global time_offset
    try:
        res = await client.futures_time()
        server_time = res["serverTime"]
        local_time = int(time.time() * 1000)
        time_offset = server_time - local_time
    except Exception as e:
        print(f"❌ Ошибка sync_time_once: {e}")


async def recv_watchdog():
    global last_tick_received
    while True:
        await asyncio.sleep(30)
        silence = time.time() - last_tick_received
        if silence > 60:
            logging.warning(
                f"🛑 Нет входящих тиков более {int(silence)} сек — форсируем рестарт"
            )
            raise Exception("No aggTrade received in 60 seconds")


async def resilient_aggtrade_stream(client, symbols):
    global aiohttp_session, last_tick_received

    streams = [f"{s.lower()}@aggTrade" for s in symbols]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    reconnect_delay = 1

    while True:
        try:
            while not recv_queue.empty():
                try:
                    recv_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            await ensure_session()
            if aiohttp_session is None:
                raise RuntimeError(
                    "aiohttp_session is None after ensure_session")

            async with aiohttp_session.ws_connect(
                    url, max_msg_size=2_000_000) as ws:
                logging.info("🔗 aggTrade WS подключен")
                last_tick_received = time.time()

                # фоново: ping, контроль закрытия и тишины
                ping_task = asyncio.create_task(
                    send_ping_forever(ws, interval=60))
                closed_watchdog = asyncio.create_task(
                    ws_watchdog(ws, name="aggTrade"))
                silence_watchdog = asyncio.create_task(recv_watchdog())
                reader_task = asyncio.create_task(ws_reader(ws))
                handler_task = asyncio.create_task(ws_handler())

                try:
                    await asyncio.gather(reader_task, handler_task)
                finally:
                    for task in [ping_task, closed_watchdog, silence_watchdog]:
                        task.cancel()
                    await asyncio.gather(ping_task,
                                         closed_watchdog,
                                         silence_watchdog,
                                         return_exceptions=True)

        except Exception as e:
            err_str = str(e)
            if 'BinanceWebsocketQueueOverflow' in err_str:
                logging.warning(
                    "🚨 Binance WebSocket очередь переполнена. Немедленный рестарт..."
                )
                reconnect_delay = 0.5
            else:
                logging.warning(
                    f"❌ aggTrade упал: {e}. Рестарт через {reconnect_delay}с")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


async def aggtrade_watchdog(symbols):
    while True:
        now = time.time()
        for s in symbols:
            last = last_aggtrade_ts[s]
            if now - last > WATCHDOG_TIMEOUT:
                logging.warning(
                    f"[watchdog] {s}: нет новых трейдов более {WATCHDOG_TIMEOUT}с"
                )
        await asyncio.sleep(5)


async def ws_watchdog(ws, name="WebSocket"):
    while True:
        await asyncio.sleep(30)
        if ws.closed:
            logging.warning(f"🛑 {name} закрыт — инициируем рестарт")
            raise Exception(f"{name} WebSocket closed")


async def keep_alive_ping():
    import aiohttp
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://binance-csy7wa.fly.dev/ping") as resp:
                    await resp.text()
                    logging.info("🌐 Keep-alive ping sent")
        except Exception as e:
            logging.warning(f"⚠️ Keep-alive ping failed: {e}")
        await asyncio.sleep(45)

async def main():
    global usdt_balance

    print("\U0001F680 Бот запущен без worker-очередей")

    while not shutdown_event.is_set():
        client = None
        tasks = []
        try:
            client = await create_client_with_retry()
            await create_global_session()
            try:
                logging.info("✅ Подключение к Binance прошло успешно")
                futures_balance = await client.futures_account_balance()
                usdt_balance = next(
                    (x for x in futures_balance if x['asset'] == 'USDT'), None)
                if usdt_balance:
                    logging.info("📊 Баланс фьючерсного аккаунта: %s USDT",
                                 usdt_balance['balance'])
                else:
                    logging.warning(
                        "⚠️ Не удалось найти баланс USDT в фьючерсном аккаунте"
                    )

            except Exception as e:
                logging.error("❌ Ошибка подключения к Binance: %s",
                              str(e),
                              exc_info=True)
                raise

            exchange_info = await client.futures_exchange_info()
            symbols = [
                s['symbol'] for s in exchange_info['symbols']
                if s['contractType'] == 'PERPETUAL'
                and s['status'] == 'TRADING' and s['symbol'].endswith('USDT')
            ]

            print(f"📈 Получено {len(symbols)} торговых пар")
            symbol_meta = {}

            for s in exchange_info['symbols']:
                if s['contractType'] == 'PERPETUAL' and s[
                        'status'] == 'TRADING' and s['symbol'].endswith(
                            'USDT'):
                    symbol = s['symbol']
                    filters = {f['filterType']: f for f in s['filters']}
                    symbol_meta[symbol] = {
                        'tick_size':
                        float(filters['PRICE_FILTER']['tickSize']),
                        'price_precision':
                        s['pricePrecision'],
                        'quantity_precision':
                        s['quantityPrecision'],
                        'min_qty':
                        float(filters['LOT_SIZE']['minQty']),
                        'min_notional':
                        float(filters['MIN_NOTIONAL']['notional'])
                        if 'MIN_NOTIONAL' in filters else None,
                    }

            await sync_time_once(client)

            usdt_balance = await get_usdt_balance(client)

            tasks.append(
                asyncio.create_task(
                    force_finalize_all_minute_candles(client, symbol_meta,
                                                      API_KEY, API_SECRET,
                                                      active_positions,
                                                      active_positions_lock,
                                                      usdt_balance)))

            tasks.append(asyncio.create_task(cleanup_trade_cache()))
            tasks.append(asyncio.create_task(sync_active_positions(client)))
            tasks.append(
                asyncio.create_task(monitor_order_updates(client,
                                                          symbol_meta)))
            tasks.append(
                asyncio.create_task(resilient_aggtrade_stream(client,
                                                              symbols)))
            tasks.append(asyncio.create_task(sync_time_forever(client)))

            await shutdown_event.wait()

        except Exception as e:
            logging.error(f"⚠️ Ошибка в main: {e}")

        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            if client:
                try:
                    await client.close_connection()
                except Exception:
                    pass

            await close_global_session()
            logging.info("\U0001F501 Перезапуск main через 5 секунд...")
            await asyncio.sleep(5) 
    await asyncio.sleep(float("inf"))


async def auto_restart_every(hours=1, minutes=0):
    total_seconds = hours * 3600 + minutes * 60
    await asyncio.sleep(total_seconds)
    logging.info(f"🔁 Перезапуск бота после {hours} ч работы")
    os.execv(sys.executable, ['python'] + sys.argv)


# main.py (внизу файла)

import atexit
import traceback

@atexit.register
def goodbye():
    logging.info("👋 Процесс завершён (atexit)")


# 👇 Добавить функцию запуска HTTP сервера async
async def start_web_server():
    async def ping(request):
        return web.Response(text="pong")

    app = web.Application()
    app.router.add_get("/ping", ping)

    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()



if __name__ == '__main__':
    import asyncio
    import logging
    import traceback

    logging.getLogger("httpx").setLevel(logging.WARNING)

    async def main_with_shutdown():
        try:
            # ✅ Запускаем web сервер как background task
            await start_web_server()
            asyncio.create_task(keep_alive_ping())  
            asyncio.create_task(auto_restart_every(hours=1))
            await main()
        except KeyboardInterrupt:
            logging.info("🛑 Прерывание с клавиатуры (Ctrl+C)")
        except Exception as e:
            logging.error("🔥 Необработанная ошибка:")
            logging.error(traceback.format_exc())
        finally:
            await shutdown()
            logging.info("👋 Завершено")

    try:
        asyncio.run(main_with_shutdown())
    except Exception as e:
        logging.critical("❌ Критический выход из asyncio.run:")
        logging.critical(traceback.format_exc())


