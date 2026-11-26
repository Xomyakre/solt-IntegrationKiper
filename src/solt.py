# Драйвер протокола SQ400 (USB CDC / UART, 115200 8N1, ASCII-фреймы).
# Формат пакета: !<TX_ID>,<GROUP_ID>,<TARGET>,<CMD>,<DATA>\r\n
# PING:          !PING\r\n  →  !TX_ID,GROUP_ID\r\n
import logging
import queue
import threading
import time
from typing import Callable, Optional

import serial

logger = logging.getLogger(__name__)

BAUDRATE = 115200
RECONNECT_DELAY = 3.0    # пауза между попытками заново открыть порт, сек
REPING_INTERVAL = 10.0   # как часто повторять PING, пока база не ответила, сек


class SoltPacket:
    """Распарсенный пакет от базы. cmd=None означает ответ на PING."""
    def __init__(self, tx_id: str, group_id: str,
                 target: str = None, cmd: str = None, data: str = None):
        self.tx_id = tx_id
        self.group_id = group_id
        self.target = target
        self.cmd = cmd
        self.data = data

    def __repr__(self):
        if self.cmd:
            return f'SoltPacket(!{self.tx_id},{self.group_id},{self.target},{self.cmd},{self.data})'
        return f'SoltPacket(!{self.tx_id},{self.group_id})'


class SOLT:
    """
    Работа с базой пейджеров SQ400 через последовательный порт.

    Использование:
        solt = SOLT('COM3')
        solt.connect()
        solt.ping()          # обязательно перед командами — получает TX_ID
        solt.on('CRAD_ON', callback)
        solt.call(124)

    Команды (ping/call/cancel/register) потокобезопасны: выполняются строго
    по одной, каждая ждёт именно свой ответ. При потере связи (выдернули USB,
    перезагрузили базу) порт переоткрывается автоматически, затем повторяется PING.
    Пока TX_ID неизвестен, команды возвращают False, а не бросают исключение.
    """

    def __init__(self, port: str, baudrate: int = BAUDRATE):
        self.port = port
        self.baudrate = baudrate
        self.tx_id: Optional[str] = None    # заполняется после ping()
        self.group_id: Optional[str] = None

        self._serial: Optional[serial.Serial] = None
        self._worker: Optional[threading.Thread] = None
        self._ping_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._port_ok = threading.Event()
        self._last_ping = 0.0
        self._callbacks: dict[str, list[Callable]] = {}

        self._write_lock = threading.Lock()
        # Одна транзакция «команда → ответ» за раз: иначе при одновременных
        # вызовах (два вебхука KDS) ответы достаются не тем вызовам.
        self._txn_lock = threading.Lock()
        # Какой ответ ждёт текущая транзакция: (cmd, data). cmd=None — ответ на PING,
        # data=None — номер не сверяем. Читается из потока чтения, поэтому под замком.
        self._state_lock = threading.Lock()
        self._expected: Optional[tuple[Optional[str], Optional[str]]] = None
        self._responses: queue.Queue = queue.Queue()

    @property
    def connected(self) -> bool:
        """Порт открыт и база ответила на PING — можно отправлять команды."""
        return self._port_ok.is_set() and self.tx_id is not None

    # ------------------------------------------------------------------
    # Подключение
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Открыть порт и запустить фоновый поток чтения (он же следит за переподключением)."""
        if not self._open():
            return False
        self._stop.clear()
        # ping() вызывает сам владелец сразу после connect(), фоновый повтор — не раньше чем через REPING_INTERVAL
        self._last_ping = time.monotonic()
        self._worker = threading.Thread(target=self._run, daemon=True, name='solt-reader')
        self._worker.start()
        return True

    def disconnect(self):
        """Остановить поток чтения и закрыть порт."""
        self._stop.set()
        self._close()
        if self._worker and self._worker is not threading.current_thread():
            self._worker.join(timeout=2)
        self.tx_id = None
        self.group_id = None
        logger.info(f'Disconnected from {self.port}')

    def _open(self, quiet: bool = False) -> bool:
        try:
            self._serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1,  # readline() блокируется максимум на 1 сек, иначе поток не остановить
            )
        except serial.SerialException as e:
            # При переподключении не засоряем лог одной и той же ошибкой раз в 3 секунды
            (logger.debug if quiet else logger.error)(f'Cannot open {self.port}: {e}')
            return False
        self._port_ok.set()
        logger.info(f'Connected to {self.port} at {self.baudrate} baud')
        return True

    def _close(self):
        self._port_ok.clear()
        if self._serial:
            try:
                self._serial.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Подписка на события от базы
    # ------------------------------------------------------------------

    def on(self, cmd: str, callback: Callable[[SoltPacket], None]):
        """
        Зарегистрировать обработчик для входящих событий от пейджеров.
        Поддерживаемые команды: CANCEL, CRAD_ON, CRAD_OFF, OK_PRESS.
        Колбек вызывается из потока чтения, не из основного потока.
        """
        self._callbacks.setdefault(cmd.upper(), []).append(callback)

    # ------------------------------------------------------------------
    # Команды PC → База
    # ------------------------------------------------------------------

    def ping(self, timeout: float = 3.0) -> Optional[tuple[str, str]]:
        """Проверить связь с базой. Возвращает (tx_id, group_id) или None."""
        self._last_ping = time.monotonic()
        packet = self._transaction('!PING\r\n', cmd=None, data=None, timeout=timeout)
        if packet:
            self.tx_id = packet.tx_id
            self.group_id = packet.group_id
            logger.info(f'Ping OK  TX_ID={self.tx_id}  GROUP_ID={self.group_id}')
            return self.tx_id, self.group_id
        logger.warning('Ping: no response')
        return None

    def call(self, number: int, timeout: float = 3.0) -> bool:
        """Вызвать пейджер SG с номером number. True — база подтвердила."""
        if not self._ready('CALL'):
            return False
        packet = self._transaction(f'!{self.tx_id},PC,SG,CALL,{number}\r\n',
                                   cmd='CALL', data=str(number), timeout=timeout)
        ok = packet is not None
        logger.info(f'CALL {number}: {"OK" if ok else "FAIL"}')
        return ok

    def cancel(self, number: int, timeout: float = 3.0) -> bool:
        """Отменить вызов SP с номером number. True — база подтвердила."""
        if not self._ready('CANCEL'):
            return False
        packet = self._transaction(f'!{self.tx_id},PC,SP,CANCEL,{number}\r\n',
                                   cmd='CANCEL', data=str(number), timeout=timeout)
        ok = packet is not None
        logger.info(f'CANCEL {number}: {"OK" if ok else "FAIL"}')
        return ok

    def register(self, number, timeout: float = 3.0) -> bool:
        """Зарегистрировать пейджер SG. number — 1-9999 или '*' для всех."""
        if not self._ready('REG'):
            return False
        # Номер в ответе на REG не сверяем: для '*' база может ответить иначе
        packet = self._transaction(f'!{self.tx_id},PC,SG,REG,{number}\r\n',
                                   cmd='REG', data=None, timeout=timeout)
        ok = packet is not None
        logger.info(f'REG {number}: {"OK" if ok else "FAIL"}')
        return ok

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _ready(self, cmd: str) -> bool:
        if self.connected:
            return True
        logger.error(f'{cmd}: нет связи с базой (порт закрыт или не выполнен PING)')
        return False

    def _send_raw(self, text: str):
        with self._write_lock:
            if not self._port_ok.is_set() or not self._serial or not self._serial.is_open:
                raise RuntimeError('Порт не открыт')
            self._serial.write(text.encode('ascii'))
            logger.debug(f'-> {text.strip()}')

    def _transaction(self, text: str, cmd: Optional[str], data: Optional[str],
                     timeout: float) -> Optional[SoltPacket]:
        """Отправить команду и дождаться именно её ответа. None — таймаут или ошибка порта."""
        with self._txn_lock:
            # Выбрасываем ответы, опоздавшие к прошлым транзакциям
            while True:
                try:
                    self._responses.get_nowait()
                except queue.Empty:
                    break
            # Ожидание регистрируем до отправки, чтобы не пропустить быстрый ответ
            with self._state_lock:
                self._expected = (cmd, data)
            try:
                self._send_raw(text)
                return self._responses.get(timeout=timeout)
            except queue.Empty:
                logger.warning(f'Нет ответа от базы на {cmd or "PING"} (timeout)')
                return None
            except (RuntimeError, serial.SerialException, OSError) as e:
                logger.error(f'Не удалось отправить {cmd or "PING"}: {e}')
                return None
            finally:
                # Снимаем ожидание и при таймауте: иначе следующее событие пейджера
                # с той же командой (например, CANCEL) ушло бы в очередь, а не в колбек
                with self._state_lock:
                    self._expected = None

    def _take_if_expected(self, packet: SoltPacket) -> bool:
        """Если пакет — ответ на текущую транзакцию, передать его ей."""
        with self._state_lock:
            if self._expected is None:
                return False
            cmd, data = self._expected
            if cmd is None:
                matched = packet.cmd is None
            else:
                matched = (packet.cmd or '').upper() == cmd and (data is None or packet.data == data)
            if matched:
                self._expected = None
                self._responses.put(packet)
            return matched

    def _parse(self, line: str) -> Optional[SoltPacket]:
        line = line.strip()
        if not line.startswith('!'):
            return None
        parts = line[1:].split(',')
        if len(parts) == 2:
            # Ответ на PING: !TX_ID,GROUP_ID
            return SoltPacket(tx_id=parts[0], group_id=parts[1])
        if len(parts) == 5:
            return SoltPacket(tx_id=parts[0], group_id=parts[1],
                              target=parts[2], cmd=parts[3], data=parts[4])
        logger.warning(f'Пакет с неверным числом полей проигнорирован: {line!r}')
        return None

    def _handle_line(self, raw: bytes):
        """
        Маршрутизация входящего пакета:
        - ответ на текущую команду PC → в транзакцию (забирает вызывающий метод)
        - события от пейджеров → колбеки, зарегистрированные через on()
        - остальное → только в отладочный лог
        """
        line = raw.decode('ascii', errors='replace').strip()
        if not line:
            return
        logger.debug(f'<- {line}')
        packet = self._parse(line)
        if packet is None or self._take_if_expected(packet):
            return
        cmd = (packet.cmd or '').upper()
        callbacks = self._callbacks.get(cmd)
        if not callbacks:
            logger.debug(f'Пакет без обработчика: {packet}')
            return
        for cb in callbacks:
            try:
                cb(packet)
            except Exception as e:
                logger.error(f'Ошибка колбека [{cmd}]: {e}')

    def _schedule_ping(self):
        """PING из отдельного потока: его ответ читает поток чтения, который и вызывает этот метод."""
        self._last_ping = time.monotonic()
        if self._ping_thread and self._ping_thread.is_alive():
            return
        self._ping_thread = threading.Thread(target=self.ping, daemon=True, name='solt-ping')
        self._ping_thread.start()

    def _run(self):
        """Фоновый поток: читает порт, при обрыве переоткрывает его и повторяет PING."""
        while not self._stop.is_set():
            if not self._port_ok.is_set():
                if self._stop.wait(RECONNECT_DELAY):
                    break
                if self._open(quiet=True):
                    self._schedule_ping()
                continue
            try:
                raw = self._serial.readline()
            except Exception as e:
                if self._stop.is_set():
                    break
                logger.error(f'Связь с базой потеряна ({e}), переподключение каждые {RECONNECT_DELAY:.0f} с')
                self.tx_id = None
                self._close()
                continue
            if raw:
                self._handle_line(raw)
            if self.tx_id is None and time.monotonic() - self._last_ping >= REPING_INTERVAL:
                self._schedule_ping()
