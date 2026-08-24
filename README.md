# PULSE scanner backend

Persistent Solana WebSocket scanner and API for the PULSE Pump.fun dashboard.

## Endpoints

- `GET /health` — deployment and integration status
- `GET /api/signals` — current scanner snapshot
- `GET /api/stream` — live Server-Sent Events
- `POST /api/telegram/test` — send a protected Telegram test alert

Deploy with the included Render Blueprint. Set `PUMP_PROGRAM_ID`,
`TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID` in Render's secret settings.

