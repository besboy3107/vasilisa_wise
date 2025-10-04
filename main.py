#!/usr/bin/env python3
"""
Главный файл для запуска Telegram-бота и админ-панели
"""
import asyncio
import uvicorn
from multiprocessing import Process
import logging
from config import Config

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def run_bot():
    """Запуск Telegram-бота"""
    from bot import EquipmentBot
    bot = EquipmentBot()
    # Синхронный запуск бота (внутри использует run_polling)
    bot.run()

def run_admin_panel():
    """Запуск админ-панели"""
    uvicorn.run(
        "admin_panel:app",
        host=Config.ADMIN_PANEL_HOST,
        port=Config.ADMIN_PANEL_PORT,
        reload=False
    )

def main():
    """Главная функция"""
    print("🚀 Запуск Equipment Bot...")
    print(f"📱 Telegram Bot Token: {'*' * 20}{Config.BOT_TOKEN[-4:] if Config.BOT_TOKEN else 'НЕ УСТАНОВЛЕН'}")
    print(f"🌐 Admin Panel: http://{Config.ADMIN_PANEL_HOST}:{Config.ADMIN_PANEL_PORT}")
    print("=" * 50)
    
    if not Config.BOT_TOKEN:
        print("❌ ОШИБКА: Не установлен BOT_TOKEN в переменных окружения!")
        print("Создайте файл .env и добавьте BOT_TOKEN=your_bot_token_here")
        return
    
    # Запускаем бота и админ-панель в отдельных процессах
    bot_process = Process(target=run_bot)
    admin_process = Process(target=run_admin_panel)
    
    try:
        bot_process.start()
        admin_process.start()
        
        print("✅ Бот и админ-панель запущены!")
        print("Нажмите Ctrl+C для остановки")
        
        # Ждем завершения процессов
        bot_process.join()
        admin_process.join()
        
    except KeyboardInterrupt:
        print("\n🛑 Получен сигнал завершения...")
        bot_process.terminate()
        admin_process.terminate()
        bot_process.join()
        admin_process.join()
        print("✅ Все процессы остановлены")

if __name__ == "__main__":
    main()

