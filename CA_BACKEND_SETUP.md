# Backend для ЦА

Этот проект остается статическим `index.html`, но для настоящего ЦА нужен backend proxy. Он хранит cookie/token на сервере и отправляет запросы в:

```text
https://ai-test.erg.kz/api/assistant-core/chats/ask-assistant
```

## Локальный запуск

```powershell
cd C:\prototip
$env:CA_COOKIE='FAIR_SPA_SESSION=...'
$env:CA_AUTH_TOKEN='Bearer ...'
npm start
```

Открыть сайт:

```text
http://127.0.0.1:3001/index.html
```

Проверить настройки proxy:

```text
http://127.0.0.1:3001/api/assistant-status
```

## Деплой backend

Можно использовать Render/Railway/VPS/корпоративный сервер.

Переменные окружения:

```text
CA_COOKIE=FAIR_SPA_SESSION=...
CA_AUTH_TOKEN=Bearer ...
CA_ASSISTANT_ID=6500
CA_UPSTREAM_URL=https://ai-test.erg.kz/api/assistant-core/chats/ask-assistant
CA_ALLOWED_ORIGINS=https://ayaulymkenzhekul.github.io
```

Если `ai-test.erg.kz` доступен только из корпоративной сети, backend тоже должен быть в этой сети. Внешний Render/Railway может не иметь доступа.

## Подключить GitHub Pages к backend

После деплоя backend, например:

```text
https://defect-assistant-proxy.onrender.com
```

Открой GitHub Pages один раз так:

```text
https://ayaulymkenzhekul.github.io/prototip/?ca_proxy=https://defect-assistant-proxy.onrender.com/api/defect-assistant
```

Сайт сохранит адрес proxy в `localStorage`.

Чтобы поменять адрес позже, открой ссылку снова с новым `ca_proxy`.

## Важно

Если прямой запрос в Postman на `ai-test.erg.kz` возвращает `401 Unauthorized`, proxy тоже не сможет получить ответ. Нужно обновить cookie/token или попросить разработчика дать правильный способ авторизации.
