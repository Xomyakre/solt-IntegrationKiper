"""
Веб-интерфейс для управления базой пейджеров SQ400.
Запуск: python web_demo.py [COM_PORT]
Открыть: http://localhost:8000
"""
import asyncio
import configparser
import json
import logging
import os
import re
import secrets
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

import uvicorn
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

from rk7 import RK7
from solt import SOLT, SoltPacket

# ------------------------------------------------------------------
# Мост logging → SSE
# Все записи логов (в т.ч. из потока чтения solt.py) рассылаются
# открытым вкладкам браузера через /events. У каждой вкладки своя
# очередь ограниченного размера: если вкладок нет, ничего не копится.
# ------------------------------------------------------------------

SSE_HISTORY = 200      # сколько последних строк показать вкладке при подключении
SSE_QUEUE_SIZE = 500   # если вкладка не успевает читать, лишние строки отбрасываются

_loop: asyncio.AbstractEventLoop | None = None
_subscribers: set[asyncio.Queue] = set()
_history: deque = deque(maxlen=SSE_HISTORY)


def _broadcast(item: dict):
    _history.append(item)
    for q in list(_subscribers):
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            pass


class _SseHandler(logging.Handler):
    def emit(self, record):
        loop = _loop
        if loop is None or loop.is_closed():
            return
        item = {'level': record.levelname, 'msg': self.format(record)}
        try:
            # call_soon_threadsafe: единственный безопасный способ передать данные
            # в event loop из стороннего потока (reader-поток SOLT)
            loop.call_soon_threadsafe(_broadcast, item)
        except RuntimeError:
            pass  # loop уже закрывается


# Каталог, где лежит сам exe/скрипт - конфиг и логи всегда рядом с ним,
# независимо от текущей рабочей директории (важно при запуске из автозагрузки/планировщика).
if getattr(sys, 'frozen', False):
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_FILE = os.path.join(APP_DIR, 'config.ini')

# Значения по умолчанию: программа запустится и без config.ini и создаст его сама
_config = configparser.ConfigParser()
_config.read_dict({
    'Web': {'port': '8000', 'admin_user': 'admin', 'admin_password': ''},
    'RK': {'host': '127.0.0.1', 'port': '8088', 'user': '', 'password': '', 'orderidentifier': 'SeqNum'},
    'ComPort': {'port': 'COM1'},
    'Log': {'log_dir': 'log', 'log_count': '10', 'log_interval': '1'},
})
_config_exists = os.path.exists(CONFIG_FILE)
_config.read(CONFIG_FILE, encoding='utf-8')


def _write_config():
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        _config.write(f)


if not _config_exists:
    _write_config()

LOG_DIR = os.path.join(APP_DIR, _config['Log']['log_dir'])
os.makedirs(LOG_DIR, exist_ok=True)
_file_handler = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, 'web_demo.log'), when='d',
    interval=_config.getint('Log', 'log_interval'), backupCount=_config.getint('Log', 'log_count'),
    encoding='utf-8')
_file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

_sse_handler = _SseHandler()
_sse_handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', datefmt='%H:%M:%S'))
logging.getLogger().addHandler(_sse_handler)
logging.getLogger().addHandler(_file_handler)
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Состояние
# ------------------------------------------------------------------

solt: SOLT | None = None
rk7 = RK7(host=_config['RK']['host'], port=_config['RK']['port'],
          user=_config['RK']['user'], password=_config['RK']['password'])
# какой идентификатор заказа передавать на пейджер: SeqNum / OrderNum
ORDER_IDENTIFIER = _config['RK']['orderidentifier']
ORDER_IDENTIFIERS = ('SeqNum', 'OrderNum')

# COM-порт по умолчанию: аргумент командной строки переопределяет сохранённый в config.ini
DEFAULT_PORT = sys.argv[1] if len(sys.argv) > 1 else _config['ComPort']['port']

PAGER_MIN, PAGER_MAX = 1, 9999

# Статусы заказа, приходящие от модуля RKeeper KDS в поле "state" вебхука.
# 0 + paid=1 — новый оплаченный заказ (CALL: пейджеры на зарядной станции получают номер)
# 4          — заказ собран (CALL: вызов пейджера)
# 5          — заказ выдан (CANCEL: сброс пейджера)
KDS_STATE_NEW = '0'
KDS_STATE_ASSEMBLED = '4'
KDS_STATE_ISSUED = '5'

# Повтор того же статуса того же заказа в течение окна считаем дублем вебхука
# и не звоним второй раз. Новый статус или повтор позже окна обрабатываются.
DUPLICATE_WINDOW = 30.0
_recent_events: dict[tuple[str, str], float] = {}


def _save_rk_config(host: str, port: str, user: str, password: str, order_identifier: str):
    """Сохранить реквизиты подключения к кассовому серверу RK7 в config.ini и применить их."""
    global rk7, ORDER_IDENTIFIER
    _config['RK']['host'] = host
    _config['RK']['port'] = port
    _config['RK']['user'] = user
    _config['RK']['password'] = password
    _config['RK']['orderidentifier'] = order_identifier
    _write_config()
    rk7 = RK7(host=host, port=port, user=user, password=password)
    ORDER_IDENTIFIER = order_identifier


def _resolve_pager_number(order: dict) -> Optional[int]:
    """Достать номер для вызова пейджера из заказа RK7 согласно настройке ORDER_IDENTIFIER."""
    if ORDER_IDENTIFIER == 'OrderNum':
        raw = order.get('orderIdent')
    else:
        raw = order.get('seqNumber')
    if raw is None:
        return None
    raw = str(raw).strip()
    # метка очереди может быть буквенно-цифровой (например "A38") - берём числовую часть
    digits = re.sub(r'\D', '', raw)
    if not digits:
        return None
    number = int(digits)
    if digits != raw:
        logger.info(f'[KDS] номер "{raw}" содержит не только цифры, на пейджер уйдёт {number} '
                    f'(заказы с разными буквами и одинаковыми цифрами получат один номер)')
    if not PAGER_MIN <= number <= PAGER_MAX:
        logger.error(f'[KDS] номер {number} вне диапазона пейджеров {PAGER_MIN}-{PAGER_MAX}')
        return None
    return number


def _is_duplicate(guid: str, state: str) -> bool:
    now = time.monotonic()
    for key, ts in list(_recent_events.items()):
        if now - ts > DUPLICATE_WINDOW:
            del _recent_events[key]
    key = (guid, state)
    if key in _recent_events:
        return True
    _recent_events[key] = now
    return False


def _save_com_port(port: str):
    """Сохранить COM-порт базы пейджеров в config.ini."""
    _config['ComPort']['port'] = port
    _write_config()


def _make_event_handler(cmd: str):
    """Фабрика колбеков для событий пейджеров — просто логирует с меткой [EVENT]."""
    def handler(p: SoltPacket):
        logger.info(f'[EVENT] {cmd}  pager #{p.data}')
    return handler


async def _connect_solt(port: str) -> dict:
    """Подключиться к базе: открыть порт, подписаться на события, выполнить PING."""
    global solt
    if solt:
        await asyncio.to_thread(solt.disconnect)
        solt = None
    candidate = SOLT(port=port)
    # to_thread: serial.Serial блокирующий — нельзя вызывать напрямую из async
    ok = await asyncio.to_thread(candidate.connect)
    if not ok:
        return {'ok': False, 'error': f'Cannot open {port}'}
    # Порт открыт — оставляем объект, даже если база пока молчит:
    # он сам будет повторять PING и переподключаться
    solt = candidate
    solt.on('CANCEL',   _make_event_handler('CANCEL'))
    solt.on('CRAD_ON',  _make_event_handler('CRAD_ON'))
    solt.on('CRAD_OFF', _make_event_handler('CRAD_OFF'))
    solt.on('OK_PRESS', _make_event_handler('OK_PRESS'))
    result = await asyncio.to_thread(solt.ping)
    if not result:
        return {'ok': False, 'error': f'Порт {port} открыт, но база не ответила на PING'}
    return {'ok': True, 'tx_id': result[0], 'group_id': result[1]}


# ------------------------------------------------------------------
# Доступ к веб-панели
# Если в [Web] задан admin_password, панель и её API требуют логин/пароль
# (HTTP Basic). Вебхук /kds открыт всегда — модуль KDS авторизоваться не умеет.
# ------------------------------------------------------------------

_basic = HTTPBasic(auto_error=False)


def require_auth(credentials: Optional[HTTPBasicCredentials] = Depends(_basic)):
    password = _config['Web'].get('admin_password', '')
    if not password:
        return
    user = _config['Web'].get('admin_user', 'admin')
    if credentials and \
            secrets.compare_digest(credentials.username.encode(), user.encode()) and \
            secrets.compare_digest(credentials.password.encode(), password.encode()):
        return
    raise HTTPException(status_code=401, detail='Unauthorized',
                        headers={'WWW-Authenticate': 'Basic realm="SOLT Pager"'})


# ------------------------------------------------------------------
# Жизненный цикл приложения
# ------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _loop
    # Сохраняем event loop здесь: на старте гарантированно получаем тот самый loop,
    # который uvicorn будет использовать. Нужен для call_soon_threadsafe из других потоков.
    _loop = asyncio.get_running_loop()
    logger.info(f'Web demo started. Default port: {DEFAULT_PORT}')
    if not _config_exists:
        logger.warning(f'config.ini не найден, создан с настройками по умолчанию: {CONFIG_FILE}')
    if not _config['Web'].get('admin_password', ''):
        logger.warning('В config.ini не задан [Web] admin_password — веб-панель доступна всем в сети без пароля')
    result = await _connect_solt(DEFAULT_PORT)
    if not result['ok']:
        logger.warning(f'Автоподключение к {DEFAULT_PORT} не удалось: {result.get("error")}')
    yield
    if solt:
        solt.disconnect()


app = FastAPI(lifespan=lifespan)
panel = APIRouter(dependencies=[Depends(require_auth)])

# ------------------------------------------------------------------
# API эндпоинты
# ------------------------------------------------------------------

@panel.get('/status')
async def status():
    return {
        'connected': solt is not None and solt.connected,
        'port': solt.port if solt else _config['ComPort']['port'],
        'tx_id': solt.tx_id if solt else None,
        'group_id': solt.group_id if solt else None,
    }


@panel.post('/connect')
async def connect(port: str = DEFAULT_PORT):
    """Подключиться к базе: открыть порт, подписаться на события, выполнить PING."""
    result = await _connect_solt(port)
    if result['ok']:
        _save_com_port(port)
    return result


@panel.post('/disconnect')
async def disconnect():
    global solt
    if solt:
        await asyncio.to_thread(solt.disconnect)
        solt = None
    return {'ok': True}


@panel.post('/ping')
async def ping():
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    result = await asyncio.to_thread(solt.ping)
    return {'ok': result is not None, 'tx_id': result[0] if result else None,
            'group_id': result[1] if result else None}


@panel.post('/call/{number}')
async def call(number: int):
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    ok = await asyncio.to_thread(solt.call, number)
    return {'ok': ok}


@panel.post('/cancel/{number}')
async def cancel(number: int):
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    ok = await asyncio.to_thread(solt.cancel, number)
    return {'ok': ok}


@panel.post('/reg/{number}')
async def reg(number: str):
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    ok = await asyncio.to_thread(solt.register, number)
    return {'ok': ok}


class RKConfig(BaseModel):
    host: str
    port: str
    user: str
    password: str = ''   # пусто — оставить сохранённый пароль
    order_identifier: str


@panel.get('/rk/config')
async def get_rk_config():
    # Пароль наружу не отдаём, только признак, что он задан
    return {'host': rk7.host, 'port': rk7.port, 'user': rk7.user, 'password_set': bool(rk7.password),
            'order_identifier': ORDER_IDENTIFIER}


@panel.post('/rk/config')
async def set_rk_config(cfg: RKConfig):
    if cfg.order_identifier not in ORDER_IDENTIFIERS:
        return {'ok': False, 'error': f'order_identifier должен быть одним из {ORDER_IDENTIFIERS}'}
    password = cfg.password or rk7.password
    _save_rk_config(cfg.host, cfg.port, cfg.user, password, cfg.order_identifier)
    logger.info(f'[RK] config saved: {cfg.host}:{cfg.port} user={cfg.user} order_identifier={cfg.order_identifier}')
    return {'ok': True}


@app.api_route('/kds', methods=['GET', 'POST', 'PUT'])
@app.api_route('/kds/{path:path}', methods=['GET', 'POST', 'PUT'])
async def kds_webhook(request: Request, path: str = ''):
    """
    Приёмник для интеграции RKeeper KDS. Шаблон данных в KDS настроен как
    {"guid": "$guid", "state": "$state", "paid": "$ispaid"} — KDS отдаёт только
    guid заказа, без человеческого номера, поэтому номер (seqNumber) дополучаем
    запросом к кассовому серверу RK7 по этому guid.
    Всегда отвечаем 200 OK: ошибки обработки только логируем.
    """
    body = await request.body()
    try:
        body_text = body.decode('utf-8')
    except UnicodeDecodeError:
        body_text = repr(body)
    logger.info(f'[KDS] {request.method} /{request.url.path.lstrip("/")}?{request.url.query}')
    logger.info(f'[KDS] body: {body_text}')
    try:
        await _handle_kds_event(body_text)
    except Exception:
        logger.exception('[KDS] ошибка обработки вебхука')
    return PlainTextResponse('OK')


async def _handle_kds_event(body_text: str):
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        logger.error(f'[KDS] body is not valid JSON: {body_text!r}')
        return

    guid = payload.get('guid')
    state = str(payload.get('state'))
    paid = str(payload.get('paid'))
    if not guid:
        logger.error(f'[KDS] no guid in payload: {payload}')
        return

    if state == KDS_STATE_NEW and paid == '1':
        action = 'call'
    elif state == KDS_STATE_ASSEMBLED:
        action = 'call'
    elif state == KDS_STATE_ISSUED:
        action = 'cancel'
    else:
        logger.info(f'[KDS] guid={guid} state={state} paid={paid}: для этого статуса действий нет')
        return

    if _is_duplicate(guid, state):
        logger.info(f'[KDS] guid={guid} state={state}: повтор за {DUPLICATE_WINDOW:.0f} с, пропускаем')
        return

    order = await asyncio.to_thread(rk7.get_order, guid)
    if order is None:
        logger.error(f'[KDS] RK7: order not found for guid={guid}')
        _recent_events.pop((guid, state), None)  # дать шанс повторному вебхуку
        return
    number = _resolve_pager_number(order)
    logger.info(f'[KDS] guid={guid} state={state} paid={paid} -> seqNumber={order.get("seqNumber")} '
                f'orderIdent={order.get("orderIdent")} '
                f'-> pager={number} (по {ORDER_IDENTIFIER}) finished={order.get("finished")}')

    if number is None:
        logger.error(f'[KDS] не удалось получить номер пейджера ({ORDER_IDENTIFIER}) для guid={guid}')
        return
    if not solt or not solt.connected:
        logger.warning(f'[KDS] нет связи с базой SOLT, пейджер №{number} не вызван (state={state})')
        _recent_events.pop((guid, state), None)
        return

    method = solt.call if action == 'call' else solt.cancel
    ok = await asyncio.to_thread(method, number)
    if not ok:
        logger.error(f'[KDS] база не подтвердила {action.upper()} для пейджера №{number}')
        _recent_events.pop((guid, state), None)


@panel.get('/events')
async def events():
    """
    SSE-стрим логов для браузера.
    X-Accel-Buffering: no — отключает буферизацию в nginx, если стоит перед приложением.
    """
    q: asyncio.Queue = asyncio.Queue(maxsize=SSE_QUEUE_SIZE)
    for item in _history:
        q.put_nowait(item)
    _subscribers.add(q)

    async def generate():
        try:
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ': keepalive\n\n'  # комментарий SSE: держит соединение живым
                    continue
                yield f'data: {json.dumps(item)}\n\n'
        finally:
            # вкладку закрыли — очередь больше не наполняем
            _subscribers.discard(q)

    return StreamingResponse(generate(), media_type='text/event-stream',
                             headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


# ------------------------------------------------------------------
# Фронтенд (встроен в строку, чтобы не нужны были статические файлы)
# ------------------------------------------------------------------

HTML = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>SOLT Pager Demo</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Consolas', 'Courier New', monospace; background: #1e1e1e; color: #d4d4d4; padding: 24px; }
h1 { color: #569cd6; font-size: 20px; margin-bottom: 20px; letter-spacing: 1px; }
.panels { display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
.panel { background: #252526; border: 1px solid #3c3c3c; border-radius: 6px; padding: 16px; flex: 1; min-width: 280px; }
.panel h2 { font-size: 13px; color: #9cdcfe; margin-bottom: 14px; text-transform: uppercase; letter-spacing: 1px; }
label { font-size: 12px; color: #888; display: block; margin-bottom: 4px; }
input[type=text], input[type=number], input[type=password], select {
    background: #3c3c3c; border: 1px solid #555; color: #d4d4d4;
    padding: 6px 10px; border-radius: 4px; font-family: inherit; font-size: 13px;
    outline: none; width: 100px;
}
select { width: auto; }
input:focus, select:focus { border-color: #569cd6; }
.row { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
button {
    background: #0e639c; color: #fff; border: none; padding: 7px 16px;
    border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 13px;
    transition: background .15s;
}
button:hover  { background: #1177bb; }
button.red    { background: #7a2020; }
button.red:hover { background: #9a2a2a; }
button.gray   { background: #444; }
button.gray:hover { background: #555; }
button.green  { background: #1e6b1e; }
button.green:hover { background: #267526; }
.status-row { display: flex; align-items: center; gap: 10px; margin-top: 6px; font-size: 12px; color: #888; }
.dot { width: 10px; height: 10px; border-radius: 50%; background: #555; flex-shrink: 0; }
.dot.on { background: #4ec9b0; }
#device-info { color: #4ec9b0; }
#log-wrap { background: #252526; border: 1px solid #3c3c3c; border-radius: 6px; padding: 16px; }
#log-wrap h2 { font-size: 13px; color: #9cdcfe; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 1px; display: flex; justify-content: space-between; align-items: center; }
#log {
    background: #0d0d0d; border: 1px solid #2a2a2a; border-radius: 4px;
    height: 420px; overflow-y: auto; padding: 12px; font-size: 12.5px; line-height: 1.6;
}
.l-debug   { color: #555; }
.l-info    { color: #9cdcfe; }
.l-event   { color: #4ec9b0; font-weight: bold; }
.l-warning { color: #dcdcaa; }
.l-error   { color: #f48771; }
</style>
</head>
<body>

<h1>&#9632; SOLT Pager Demo</h1>

<div class="panels">

  <div class="panel">
    <h2>Подключение</h2>
    <div class="row">
      <div>
        <label>COM-порт</label>
        <input type="text" id="port" value="COM1" style="width:90px">
      </div>
      <button class="green" onclick="doConnect()" style="margin-top:16px">Подключить</button>
      <button class="red"   onclick="doDisconnect()" style="margin-top:16px">Отключить</button>
    </div>
    <div class="status-row">
      <div class="dot" id="dot"></div>
      <span id="device-info">нет связи</span>
    </div>
  </div>

  <div class="panel">
    <h2>Касса RK7</h2>
    <div class="row">
      <div>
        <label>Адрес</label>
        <input type="text" id="rk-host" style="width:140px">
      </div>
      <div>
        <label>Порт</label>
        <input type="text" id="rk-port" style="width:70px">
      </div>
    </div>
    <div class="row">
      <div>
        <label>Логин</label>
        <input type="text" id="rk-user" style="width:110px">
      </div>
      <div>
        <label>Пароль</label>
        <input type="password" id="rk-password" style="width:110px" autocomplete="new-password">
      </div>
    </div>
    <div class="row">
      <div>
        <label>Номер для пейджера</label>
        <select id="rk-order-identifier">
          <option value="SeqNum">SeqNum — порядковый номер</option>
          <option value="OrderNum">OrderNum — номер заказа</option>
        </select>
      </div>
    </div>
    <div class="row">
      <button class="green" onclick="saveRkConfig()">Сохранить</button>
      <span id="rk-config-status" style="font-size:12px;color:#888"></span>
    </div>
    <div class="status-row" style="display:block; line-height:1.7;">
      Статусы заказа (KDS state):<br>
      <b>0</b> + оплачен — новый заказ &rarr; <b>CALL</b> (инициализация пейджера)<br>
      <b>4</b> — заказ собран &rarr; <b>CALL</b> (вызов пейджера)<br>
      <b>5</b> — заказ выдан &rarr; <b>CANCEL</b> (сброс пейджера)
    </div>
  </div>

  <div class="panel">
    <h2>Команды</h2>
    <div class="row">
      <div>
        <label>Номер пейджера</label>
        <input type="number" id="number" value="1" min="1" max="9999" style="width:90px">
      </div>
    </div>
    <div class="row">
      <button onclick="doCall()">&#9658; CALL</button>
      <button class="red" onclick="doCancel()">&#10007; CANCEL</button>
    </div>
    <div class="row">
      <button class="gray" onclick="doReg()">REG №</button>
      <button class="gray" onclick="doRegAll()">REG *</button>
      <button class="gray" onclick="doPing()">PING</button>
    </div>
  </div>

</div>

<div id="log-wrap">
  <h2>Лог <button class="gray" onclick="clearLog()" style="font-size:11px;padding:4px 10px">Очистить</button></h2>
  <div id="log"></div>
</div>

<script>
const logEl  = document.getElementById('log');
const dotEl  = document.getElementById('dot');
const infoEl = document.getElementById('device-info');

function addLog(level, msg) {
    const d = document.createElement('div');
    let cls = 'l-info';
    if (level === 'DEBUG')   cls = 'l-debug';
    if (level === 'WARNING') cls = 'l-warning';
    if (level === 'ERROR')   cls = 'l-error';
    if (msg.includes('[EVENT]')) cls = 'l-event'; // события пейджеров выделяем отдельно
    d.className = cls;
    d.textContent = msg;
    logEl.appendChild(d);
    logEl.scrollTop = logEl.scrollHeight;
}

function clearLog() { logEl.innerHTML = ''; }

function setConnected(d) {
    if (d.connected || d.ok) {
        dotEl.className = 'dot on';
        infoEl.textContent = `Подключено (TX_ID: ${d.tx_id}  GROUP_ID: ${d.group_id})`;
    } else {
        dotEl.className = 'dot';
        infoEl.textContent = d.error || 'нет связи';
    }
}

// SSE: браузер сам переподключается при обрыве (стандартное поведение EventSource).
// При каждом подключении сервер заново присылает последние строки, поэтому лог очищаем.
const es = new EventSource('/events');
es.onopen    = () => clearLog();
es.onmessage = e => { const d = JSON.parse(e.data); addLog(d.level, d.msg); };
es.onerror   = () => addLog('ERROR', 'SSE connection lost');

async function api(method, url) {
    try {
        const r = await fetch(url, { method });
        return await r.json();
    } catch(e) {
        addLog('ERROR', 'fetch error: ' + e);
        return { ok: false };
    }
}

async function doConnect() {
    const port = document.getElementById('port').value.trim();
    setConnected(await api('POST', `/connect?port=${encodeURIComponent(port)}`));
}

async function doDisconnect() {
    await api('POST', '/disconnect');
    setConnected({ connected: false });
}

function getNum() { return document.getElementById('number').value; }

async function doPing()   { setConnected(await api('POST', '/ping')); }
async function doCall()   { await api('POST', `/call/${getNum()}`); }
async function doCancel() { await api('POST', `/cancel/${getNum()}`); }
async function doReg()    { await api('POST', `/reg/${getNum()}`); }
async function doRegAll() { await api('POST', '/reg/*'); }

async function loadRkConfig() {
    const r = await fetch('/rk/config');
    const d = await r.json();
    document.getElementById('rk-host').value = d.host;
    document.getElementById('rk-port').value = d.port;
    document.getElementById('rk-user').value = d.user;
    document.getElementById('rk-password').placeholder = d.password_set ? 'не менять' : 'не задан';
    document.getElementById('rk-order-identifier').value = d.order_identifier;
}

async function saveRkConfig() {
    const statusEl = document.getElementById('rk-config-status');
    const body = {
        host: document.getElementById('rk-host').value.trim(),
        port: document.getElementById('rk-port').value.trim(),
        user: document.getElementById('rk-user').value.trim(),
        password: document.getElementById('rk-password').value,
        order_identifier: document.getElementById('rk-order-identifier').value,
    };
    try {
        const r = await fetch('/rk/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const d = await r.json();
        statusEl.textContent = d.ok ? 'сохранено' : ('ошибка: ' + (d.error || ''));
        if (d.ok) {
            document.getElementById('rk-password').value = '';
            loadRkConfig();
        }
    } catch (e) {
        statusEl.textContent = 'ошибка: ' + e;
    }
}

async function loadStatus() {
    const r = await fetch('/status');
    const d = await r.json();
    document.getElementById('port').value = d.port;
    setConnected(d);
}

loadRkConfig();
loadStatus();
setInterval(loadStatus, 5000); // индикатор связи обновляется сам, в т.ч. после переподключения базы
</script>
</body>
</html>
"""


@panel.get('/', response_class=HTMLResponse)
async def index():
    return HTML


app.include_router(panel)


if __name__ == '__main__':
    # При сборке в exe с --noconsole падение до старта логирования иначе было бы не видно никак
    try:
        # log_config=None: собственная настройка логирования uvicorn падает без консоли
        # (sys.stdout is None -> AttributeError в colour-форматтере), плюс у нас уже есть
        # обработчики на корневом логгере (файл + SSE), их достаточно
        uvicorn.run(app, host='0.0.0.0', port=_config.getint('Web', 'port'),
                    log_level='warning', log_config=None)
    except Exception:
        logger.exception('Fatal error on startup')
        raise
