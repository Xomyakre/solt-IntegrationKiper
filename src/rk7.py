# Клиент RK7 XML API (HTTP-интерфейс кассового сервера RKeeper).
# Используется, чтобы по guid заказа, присланному модулем RKeeper KDS,
# получить его seqNumber (порядковый номер заказа) для вызова на пейджере.
import logging
from typing import Optional

import requests
from lxml import etree
from requests.packages.urllib3.exceptions import InsecureRequestWarning

logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

GET_ORDER_TEMPLATE = '''<?xml version="1.0" encoding="UTF-8"?>
<RK7Query>
    <RK7CMD CMD="GetOrder">
        <Order guid="{guid}"/>
    </RK7CMD>
</RK7Query>'''


def _format_guid(guid: str) -> str:
    """RK7 ожидает guid вида {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}, KDS присылает без скобок."""
    return '{%s}' % guid.strip().strip('{}')


class RK7:
    """
    HTTP-доступ к кассовому серверу RKeeper (rk7api/v0/xmlinterface.xml).

    Использование:
        rk7 = RK7(host='172.20.0.114', port='8086', user='httpuser', password='24')
        order = rk7.get_order(guid)   # {'seqNumber': '5', 'orderName': '...', ...} или None
    """

    def __init__(self, host: str, port: str, user: str, password: str, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout

    def _url(self) -> str:
        return f'https://{self.host}:{self.port}/rk7api/v0/xmlinterface.xml'

    def get_order(self, guid: str) -> Optional[dict]:
        """Вернуть атрибуты заказа (seqNumber, orderName, paid, finished, ...) по guid, или None при ошибке."""
        xml = GET_ORDER_TEMPLATE.format(guid=_format_guid(guid))
        try:
            resp = requests.get(self._url(), verify=False, auth=(self.user, self.password),
                                 data=xml.encode('utf-8'), timeout=self.timeout)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f'RK7 request failed: {e}')
            return None
        try:
            root = etree.fromstring(resp.content)
        except etree.XMLSyntaxError as e:
            logger.error(f'RK7 response parse failed: {e}')
            return None
        if root.get('Status') != 'Ok':
            logger.warning(f'RK7 GetOrder error: {root.get("ErrorText")}')
            return None
        order = root.find('Order')
        if order is None:
            logger.warning(f'RK7 GetOrder: order not found for guid {guid}')
            return None
        result = dict(order.attrib)
        result['guests'] = [g.get('guestLabel') for g in order.findall('Guests/Guest')]
        result['external_props'] = {p.get('name'): p.get('value') for p in order.findall('ExternalProps/Prop')}
        return result

    def get_order_seq_number(self, guid: str) -> Optional[int]:
        order = self.get_order(guid)
        if order is None or order.get('seqNumber') is None:
            return None
        return int(order['seqNumber'])
