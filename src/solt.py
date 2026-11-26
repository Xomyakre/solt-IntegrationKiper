# Драйвер протокола SQ400 (USB CDC / UART, 115200 8N1, ASCII-фреймы).
# Формат пакета: !<TX_ID>,<GROUP_ID>,<TARGET>,<CMD>,<DATA>\r\n
# PING:          !PING\r\n  →  !TX_ID,GROUP_ID\r\n
import logging
import queue
import threading
from typing import Callable, Optional

import serial

logger = logging.getLogger(__name__)

BAUDRATE = 115200


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
    """

    def __init__(self, port: str, baudrate: int = BAUDRATE):
        self.port = port
        self.baudrate = baudrate
        self.tx_id: Optional[str] = None    # заполняется после ping()
        self.group_id: Optional[str] = None

        self._serial: Optional[serial.Serial] = None
        self._reader: Optional[threading.Thread] = None
        self._running = False
        self._response_queue: queue.Queue = queue.Queue()
        self._callbacks: dict[str, list[Callable]] = {}
        # Команды, на эхо которых сейчас ждём ответа.
        # Нужно, чтобы reader не уводил ожидаемый ответ в колбек вместо очереди.
        self._pending_cmds: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Подключение
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Открыть порт и запустить фоновый поток чтения."""
        try:
            self._serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1,  # readline() блокируется максимум на 1 сек, иначе поток не остановить
            )
            self._running = True
            self._reader = threading.Thread(target=self._read_loop, daemon=True, name='solt-reader')
            self._reader.start()
            logger.info(f'Connected to {self.port} at {self.baudrate} baud')
            return True
        except serial.SerialException as e:
            logger.error(f'Cannot open {self.port}: {e}')
            return False

    def disconnect(self):
        """Остановить поток чтения и закрыть порт."""
        self._running = False
        if self._reader:
            self._reader.join(timeout=2)
        if self._serial and self._serial.is_open:
            self._serial.close()
        logger.info(f'Disconnected from {self.port}')

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
        self._send_raw('!PING\r\n')
        packet = self._wait(timeout=timeout)
        if packet and packet.cmd is None:
            self.tx_id = packet.tx_id
            self.group_id = packet.group_id
            logger.info(f'Ping OK  TX_ID={self.tx_id}  GROUP_ID={self.group_id}')
            return self.tx_id, self.group_id
        logger.warning('Ping: no response')
        return None

    def call(self, number: int, timeout: float = 3.0) -> bool:
        """Вызвать пейджер SG с номером number. True — база подтвердила."""
        self._require_tx_id()
        self._send_cmd(f'!{self.tx_id},PC,SG,CALL,{number}\r\n', expected='CALL')
        packet = self._wait(timeout=timeout)
        ok = packet is not None and packet.cmd == 'CALL' and packet.data == str(number)
        logger.info(f'CALL {number}: {"OK" if ok else "FAIL"}')
        return ok

    def cancel(self, number: int, timeout: float = 3.0) -> bool:
        """Отменить вызов SP с номером number. True — база подтвердила."""
        self._require_tx_id()
        self._send_cmd(f'!{self.tx_id},PC,SP,CANCEL,{number}\r\n', expected='CANCEL')
        packet = self._wait(timeout=timeout)
        ok = packet is not None and packet.cmd == 'CANCEL' and packet.data == str(number)
        logger.info(f'CANCEL {number}: {"OK" if ok else "FAIL"}')
        return ok

    def register(self, number, timeout: float = 3.0) -> bool:
        """Зарегистрировать пейджер SG. number — 1-9999 или '*' для всех."""
        self._require_tx_id()
        self._send_cmd(f'!{self.tx_id},PC,SG,REG,{number}\r\n', expected='REG')
        packet = self._wait(timeout=timeout)
        ok = packet is not None and packet.cmd == 'REG'
        logger.info(f'REG {number}: {"OK" if ok else "FAIL"}')
        return ok

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _require_tx_id(self):
        if not self.tx_id:
            raise RuntimeError('TX_ID неизвестен — сначала вызовите ping()')

    def _send_raw(self, text: str):
        # Lock нужен: несколько потоков (например, asyncio через to_thread) могут писать одновременно
        with self._lock:
            if not self._serial or not self._serial.is_open:
                raise RuntimeError('Порт не открыт')
            self._serial.write(text.encode('ascii'))
            logger.debug(f'-> {text.strip()}')

    def _send_cmd(self, text: str, expected: str):
        # Регистрируем ожидание до отправки, чтобы не пропустить быстрый ответ
        self._pending_cmds.add(expected.upper())
        self._send_raw(text)

    def _wait(self, timeout: float) -> Optional[SoltPacket]:
        try:
            return self._response_queue.get(timeout=timeout)
        except queue.Empty:
            logger.warning('Нет ответа от базы (timeout)')
            return None

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

    def _read_loop(self):
        """
        Фоновый поток: читает пакеты и роутит их:
        - ожидаемые эхо-ответы на команды PC → _response_queue (забирает вызывающий метод)
        - спонтанные события от пейджеров → колбеки, зарегистрированные через on()
        """
        while self._running:
            try:
                raw = self._serial.readline()
                if not raw:
                    continue
                line = raw.decode('ascii', errors='replace').strip()
                if not line:
                    continue
                logger.debug(f'<- {line}')
                packet = self._parse(line)
                if packet is None:
                    continue
                cmd = (packet.cmd or '').upper()
                if not cmd or cmd in self._pending_cmds:
                    # Ответ на нашу команду (или PING)
                    self._pending_cmds.discard(cmd)
                    self._response_queue.put(packet)
                elif cmd in self._callbacks:
                    # Событие от пейджера
                    for cb in self._callbacks[cmd]:
                        try:
                            cb(packet)
                        except Exception as e:
                            logger.error(f'Ошибка колбека [{cmd}]: {e}')
                else:
                    # Неизвестная команда — всё равно кладём в очередь, не теряем
                    self._response_queue.put(packet)
            except Exception as e:
                if self._running:
                    logger.error(f'Ошибка чтения порта: {e}')
