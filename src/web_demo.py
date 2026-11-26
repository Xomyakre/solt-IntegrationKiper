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
import sys
from contextlib import asynccontextmanager
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, PlainTextResponse
from pydantic import BaseModel

from rk7 import RK7
from solt import SOLT, SoltPacket

# ------------------------------------------------------------------
# Мост logging → SSE
# Все записи логов (в т.ч. из потока чтения solt.py) попадают в
# _sse_queue и стримятся в браузер через /events.
# ------------------------------------------------------------------

_loop: asyncio.AbstractEventLoop | None = None
_sse_queue: asyncio.Queue | None = None


class _SseHandler(logging.Handler):
    def emit(self, record):
        if _loop and _sse_queue:
            # call_soon_threadsafe: единственный безопасный способ положить данные
            # в asyncio.Queue из стороннего потока (reader-поток SOLT)
            _loop.call_soon_threadsafe(
                _sse_queue.put_nowait,
                {'level': record.levelname, 'msg': self.format(record)}
            )


# Каталог, где лежит сам exe/скрипт - конфиг и логи всегда рядом с ним,
# независимо от текущей рабочей директории (важно при запуске из автозагрузки/планировщика).
if getattr(sys, 'frozen', False):
    APP_DIR = os.path.dirname(sys.executable)
else:
    APP_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_FILE = os.path.join(APP_DIR, 'config.ini')

_config = configparser.ConfigParser()
_config.read(CONFIG_FILE, encoding='utf-8')

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
rk7 = RK7(host=_config['RK']['Host'], port=_config['RK']['Port'],
          user=_config['RK']['User'], password=_config['RK']['Password'])
# какой идентификатор заказа передавать на пейджер: SeqNum / OrderNum
ORDER_IDENTIFIER = _config['RK']['OrderIdentifier']

# COM-порт по умолчанию: аргумент командной строки переопределяет сохранённый в config.ini
DEFAULT_PORT = sys.argv[1] if len(sys.argv) > 1 else _config['ComPort']['Port']


# Статусы заказа, приходящие от модуля RKeeper KDS в поле "state" вебхука.
# 0 + paid=1 — новый оплаченный заказ (CALL: инициализация пейджера)
# 4          — заказ собран (CALL: вызов пейджера)
# 5          — заказ выдан (CANCEL: сброс пейджера)
KDS_STATE_NEW = '0'
KDS_STATE_ASSEMBLED = '4'
KDS_STATE_ISSUED = '5'


def _save_rk_config(host: str, port: str, user: str, password: str, order_identifier: str):
    """Сохранить реквизиты подключения к кассовому серверу RK7 в config.ini и применить их."""
    global rk7, ORDER_IDENTIFIER
    _config['RK']['Host'] = host
    _config['RK']['Port'] = port
    _config['RK']['User'] = user
    _config['RK']['Password'] = password
    _config['RK']['OrderIdentifier'] = order_identifier
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        _config.write(f)
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
    # метка очереди может быть буквенно-цифровой (например "A38") - берём числовую часть
    digits = re.sub(r'\D', '', str(raw))
    return int(digits) if digits else None


def _save_com_port(port: str):
    """Сохранить COM-порт базы пейджеров в config.ini."""
    _config['ComPort']['Port'] = port
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        _config.write(f)


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
    solt = candidate
    solt.on('CANCEL',   _make_event_handler('CANCEL'))
    solt.on('CRAD_ON',  _make_event_handler('CRAD_ON'))
    solt.on('CRAD_OFF', _make_event_handler('CRAD_OFF'))
    solt.on('OK_PRESS', _make_event_handler('OK_PRESS'))
    result = await asyncio.to_thread(solt.ping)
    return {'ok': True, 'tx_id': result[0] if result else '?', 'group_id': result[1] if result else '?'}


# ------------------------------------------------------------------
# Жизненный цикл приложения
# ------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _loop, _sse_queue
    # Сохраняем event loop здесь: на старте гарантированно получаем тот самый loop,
    # который uvicorn будет использовать. Нужен для call_soon_threadsafe из других потоков.
    _loop = asyncio.get_event_loop()
    _sse_queue = asyncio.Queue()
    logger.info(f'Web demo started. Default port: {DEFAULT_PORT}')
    result = await _connect_solt(DEFAULT_PORT)
    if not result['ok']:
        logger.warning(f'Автоподключение к {DEFAULT_PORT} не удалось: {result.get("error")}')
    yield
    if solt:
        solt.disconnect()


app = FastAPI(lifespan=lifespan)

# ------------------------------------------------------------------
# API эндпоинты
# ------------------------------------------------------------------

@app.get('/status')
async def status():
    return {
        'connected': solt is not None,
        'port': solt.port if solt else _config['ComPort']['Port'],
        'tx_id': solt.tx_id if solt else None,
        'group_id': solt.group_id if solt else None,
    }


@app.post('/connect')
async def connect(port: str = DEFAULT_PORT):
    """Подключиться к базе: открыть порт, подписаться на события, выполнить PING."""
    result = await _connect_solt(port)
    if result['ok']:
        _save_com_port(port)
    return result


@app.post('/disconnect')
async def disconnect():
    global solt
    if solt:
        await asyncio.to_thread(solt.disconnect)
        solt = None
    return {'ok': True}


@app.post('/ping')
async def ping():
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    result = await asyncio.to_thread(solt.ping)
    return {'ok': result is not None, 'tx_id': result[0] if result else None}


@app.post('/call/{number}')
async def call(number: int):
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    ok = await asyncio.to_thread(solt.call, number)
    return {'ok': ok}


@app.post('/cancel/{number}')
async def cancel(number: int):
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    ok = await asyncio.to_thread(solt.cancel, number)
    return {'ok': ok}


@app.post('/reg/{number}')
async def reg(number: str):
    if not solt:
        return {'ok': False, 'error': 'not connected'}
    ok = await asyncio.to_thread(solt.register, number)
    return {'ok': ok}


class RKConfig(BaseModel):
    host: str
    port: str
    user: str
    password: str
    order_identifier: str


@app.get('/rk/config')
async def get_rk_config():
    return {'host': rk7.host, 'port': rk7.port, 'user': rk7.user, 'password': rk7.password,
            'order_identifier': ORDER_IDENTIFIER}


@app.post('/rk/config')
async def set_rk_config(cfg: RKConfig):
    _save_rk_config(cfg.host, cfg.port, cfg.user, cfg.password, cfg.order_identifier)
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
    """
    body = await request.body()
    try:
        body_text = body.decode('utf-8')
    except UnicodeDecodeError:
        body_text = repr(body)
    logger.info(f'[KDS] {request.method} /{request.url.path.lstrip("/")}?{request.url.query}')
    logger.info(f'[KDS] body: {body_text}')

    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        logger.error(f'[KDS] body is not valid JSON: {body_text!r}')
        return PlainTextResponse('OK')

    guid = payload.get('guid')
    state = payload.get('state')
    paid = payload.get('paid')
    if not guid:
        logger.error(f'[KDS] no guid in payload: {payload}')
        return PlainTextResponse('OK')

    order = await asyncio.to_thread(rk7.get_order, guid)
    if order is None:
        logger.error(f'[KDS] RK7: order not found for guid={guid}')
        return PlainTextResponse('OK')
    number = _resolve_pager_number(order)
    logger.info(f'[KDS] guid={guid} state={state} paid={paid} -> seqNumber={order.get("seqNumber")} '
                f'orderIdent={order.get("orderIdent")} '
                f'-> pager={number} (по {ORDER_IDENTIFIER}) finished={order.get("finished")}')

    if number is None:
        logger.error(f'[KDS] не удалось получить номер пейджера ({ORDER_IDENTIFIER}) для guid={guid}')
        return PlainTextResponse('OK')
    if not solt:
        logger.warning(f'[KDS] SOLT не подключен, пейджер №{number} не вызван (state={state})')
        return PlainTextResponse('OK')

    state = str(state)
    if state == KDS_STATE_NEW and str(paid) == '1':
        await asyncio.to_thread(solt.call, number)
    elif state == KDS_STATE_ASSEMBLED:
        await asyncio.to_thread(solt.call, number)
    elif state == KDS_STATE_ISSUED:
        await asyncio.to_thread(solt.cancel, number)

    return PlainTextResponse('OK')


@app.get('/events')
async def events():
    """
    SSE-стрим логов для браузера.
    X-Accel-Buffering: no — отключает буферизацию в nginx, если стоит перед приложением.
    """
    async def generate():
        while True:
            item = await _sse_queue.get()
            yield f'data: {json.dumps(item)}\n\n'
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
input[type=text], input[type=number], select {
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
        <input type="text" id="rk-password" style="width:110px">
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
      <b>0</b> + оплачен — новый заказ → <b>CALL</b> (инициализация пейджера)<br>
      <b>4</b> — заказ собран → <b>CALL</b> (вызов пейджера)<br>
      <b>5</b> — заказ выдан → <b>CANCEL</b> (сброс пейджера)
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

// SSE: браузер сам переподключается при обрыве (стандартное поведение EventSource)
const es = new EventSource('/events');
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
    const d = await api('POST', `/connect?port=${encodeURIComponent(port)}`);
    if (d.ok) {
        dotEl.className = 'dot on';
        infoEl.textContent = `Подключено (TX_ID: ${d.tx_id}  GROUP_ID: ${d.group_id})`;
    } else {
        dotEl.className = 'dot';
        infoEl.textContent = d.error || 'ошибка';
    }
}

async function doDisconnect() {
    await api('POST', '/disconnect');
    dotEl.className = 'dot';
    infoEl.textContent = 'нет связи';
}

function getNum() { return document.getElementById('number').value; }

async function doPing()   { await api('POST', '/ping'); }
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
    document.getElementById('rk-password').value = d.password;
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
        statusEl.textContent = d.ok ? 'сохранено' : 'ошибка';
    } catch (e) {
        statusEl.textContent = 'ошибка: ' + e;
    }
}

async function loadStatus() {
    const r = await fetch('/status');
    const d = await r.json();
    document.getElementById('port').value = d.port;
    if (d.connected) {
        dotEl.className = 'dot on';
        infoEl.textContent = `Подключено (TX_ID: ${d.tx_id}  GROUP_ID: ${d.group_id})`;
    }
}

loadRkConfig();
loadStatus();
</script>
</body>
</html>
"""


@app.get('/', response_class=HTMLResponse)
async def index():
    return HTML


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
