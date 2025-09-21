import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram Bot Configuration
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
    
    # Database Configuration
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./equipment.db")
    
    # Web Admin Panel
    ADMIN_PANEL_PORT = int(os.getenv("ADMIN_PANEL_PORT", "8000"))
    ADMIN_PANEL_HOST = os.getenv("ADMIN_PANEL_HOST", "127.0.0.1")
    
    # Equipment Categories
    EQUIPMENT_CATEGORIES = [
        "Компьютеры и ноутбуки",
        "Серверное оборудование", 
        "Сетевое оборудование",
        "Принтеры и МФУ",
        "Мониторы и дисплеи",
        "Комплектующие",
        "Периферия",
        "Другое"
    ]

