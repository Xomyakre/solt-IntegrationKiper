# SOLT Pager Service — пейджеры SOLT SQ400 для r_keeper KDS

Сервис связывает модуль **r_keeper KDS** с базой гостевых пейджеров **SOLT SQ400** (USB/COM):

1. Кассир пробивает и оплачивает заказ → KDS присылает вебхук `state=0, paid=1` → пейджеры на зарядной станции получают номер заказа (`CALL`).
2. Сборщик переводит заказ в «Собран» (`state=4`) → пейджер с этим номером вызывается (`CALL`).
3. Заказ «Выдан» (`state=5`) → вызов сбрасывается (`CANCEL`), если в `[Solt] cancel_target` задан `SP` или `SG`. По умолчанию `off`: в протоколе SQ400 v1.0 отмена от ПК не описана, вызов снимает гость кнопкой на пейджере или возвратом на станцию.

KDS присылает только `guid` заказа, поэтому номер (`seqNumber`) сервис получает у кассового сервера RK7 через XML-интерфейс.

```
r_keeper KDS ──вебхук──► SoltPagerService (:8000/kds) ──COM──► база SQ400 ──радио──► пейджеры
                                │
                                └──XML-интерфейс──► кассовый сервер RK7 (GetOrder → seqNumber)
```

## Состав

| Файл | Что делает |
| --- | --- |
| `src/web_demo.py` | Веб-сервер: вебхук `/kds`, веб-панель, API команд, лог в браузере |
| `src/solt.py` | Драйвер протокола SQ400 (115200 8N1, ASCII): PING, CALL, CANCEL, REG, события пейджеров, автопереподключение |
| `src/rk7.py` | Клиент XML-интерфейса RK7: `GetOrder` по guid |
| `src/config.example.ini` | Пример настроек |

## Требования

- Windows, ПК в одной сети с кассовым сервером RK7 и сервером KDS
- База SQ400, подключённая по USB (появляется как COM-порт)
- Для запуска из исходников: Python **3.10+**

## Настройка

### 1. KDS (портал kds.ucs.ru)

Настройки сервера KDS → **Интеграции**:

- тип: `externalSendStates`, включено;
- адрес: `http://<IP этого ПК>:8000/kds`;
- данные: `{"guid": "$guid", "state": "$state", "paid": "$ispaid"}`.

Логин, пароль и порт HTTP-интерфейса кассового сервера возьмите из настроек модуля KDS: сервис использует того же пользователя.

### 2. config.ini

Скопируйте `config.example.ini` в `config.ini` рядом с программой и заполните. Если файла нет, программа создаст его с настройками по умолчанию.

```ini
[Web]
port = 8000
admin_user = admin
; пароль на веб-панель (комментарии только отдельной строкой, не в конце значения)
admin_password = придумайте-пароль

[RK]
host = 172.20.0.114
port = 8088
user = httpuser
password = ********
orderidentifier = SeqNum

[ComPort]
port = COM1
```

Настройки кассы и COM-порт можно поменять и из веб-панели: они сохранятся в `config.ini`.

## Запуск

### Из исходников

```bash
cd src
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python web_demo.py          # или: python web_demo.py COM3
```

### Сборка exe

```bash
pip install pyinstaller
pyinstaller --onefile --noconsole --name SoltPagerService web_demo.py
```

Готовый файл: `dist\SoltPagerService.exe`. Положите рядом `config.ini` и добавьте exe в автозагрузку (`Win+R` → `shell:startup` → ярлык на exe).

Веб-панель: `http://<IP этого ПК>:8000/`. Логи: папка `log\` рядом с программой.

## Проверка

1. **Панель.** Откройте `http://localhost:8000/`. Если задан `admin_password`, браузер спросит логин и пароль. Индикатор должен показать «Подключено (TX_ID: … GROUP_ID: …)».
2. **База.** В блоке «Команды» нажмите `PING`, затем `CALL` на номер пейджера, который лежит на станции, затем `CANCEL`. В логе: `CALL N: OK`, `CANCEL N: OK`.
3. **Касса.** Возьмите guid реального открытого заказа и отправьте вебхук вручную (PowerShell):
   ```powershell
   Invoke-RestMethod -Method Post -Uri http://localhost:8000/kds -ContentType 'application/json' `
     -Body '{"guid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX", "state": "4", "paid": "1"}'
   ```
   В логе: `[KDS] ... -> seqNumber=… -> pager=…`, затем `CALL N: OK`. Если `RK7 request failed` или `order not found`, проверьте адрес, порт и логин кассы.
4. **Полный цикл.** Пробейте и оплатите заказ на кассе → «Собран» на KDS → «Выдан» и проверьте пейджер на каждом шаге.
5. **Переподключение.** Выдерните USB базы: в логе `Связь с базой потеряна`. Вставьте обратно: через несколько секунд `Connected to COMx` и `Ping OK`, индикатор снова зелёный.
