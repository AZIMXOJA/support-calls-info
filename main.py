"""
main.py — запускает бота и FastAPI сервер одновременно
Один процесс, один сервис на Render
"""

import asyncio
import threading
import uvicorn

# ── Запуск FastAPI в отдельном потоке ────

def run_server():
    from server import app
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")

# ── Запуск бота ──────────────────────────

async def run_bot():
    from support_bot import dp, bot
    await dp.start_polling(bot)

# ── Точка входа ──────────────────────────

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger(__name__)

    log.info("✅ Запуск сервера и бота...")

    # Сервер в отдельном потоке
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    log.info("🌐 FastAPI сервер запущен")

    # Бот в главном потоке
    log.info("🤖 Telegram бот запускается...")
    asyncio.run(run_bot())
