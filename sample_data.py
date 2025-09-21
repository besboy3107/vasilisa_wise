#!/usr/bin/env python3
"""
Скрипт для добавления примеров оборудования в базу данных
"""
import asyncio
import json
from database import init_db, async_session
from services import EquipmentService, UserService
from models import EquipmentCreate, UserCreate
from config import Config

async def add_sample_equipment():
    """Добавление примеров оборудования"""
    await init_db()
    
    sample_equipment = [
        {
            "name": "iPhone 15 Pro",
            "category": "Компьютеры и ноутбуки",
            "description": "Новейший смартфон Apple с титановым корпусом и чипом A17 Pro",
            "price": 99990,
            "currency": "RUB",
            "brand": "Apple",
            "model": "iPhone 15 Pro",
            "specifications": {
                "Экран": "6.1 дюйма Super Retina XDR",
                "Процессор": "A17 Pro",
                "Память": "128GB",
                "Камера": "48MP основная + 12MP ультраширокая",
                "Батарея": "До 23 часов видео"
            },
            "availability": True
        },
        {
            "name": "MacBook Air M2",
            "category": "Компьютеры и ноутбуки",
            "description": "Ультратонкий ноутбук с чипом M2 и дисплеем Liquid Retina",
            "price": 119990,
            "currency": "RUB",
            "brand": "Apple",
            "model": "MacBook Air M2",
            "specifications": {
                "Экран": "13.6 дюйма Liquid Retina",
                "Процессор": "Apple M2",
                "RAM": "8GB",
                "SSD": "256GB",
                "Вес": "1.24 кг"
            },
            "availability": True
        },
        {
            "name": "Dell PowerEdge R750",
            "category": "Серверное оборудование",
            "description": "Сервер 1U с процессорами Intel Xeon 3-го поколения",
            "price": 450000,
            "currency": "RUB",
            "brand": "Dell",
            "model": "PowerEdge R750",
            "specifications": {
                "Процессор": "2x Intel Xeon Silver 4314",
                "RAM": "32GB DDR4",
                "Диски": "2x 480GB SSD",
                "Сеть": "2x 1Gb Ethernet",
                "Форм-фактор": "1U"
            },
            "availability": True
        },
        {
            "name": "Cisco Catalyst 2960-X",
            "category": "Сетевое оборудование",
            "description": "Коммутатор уровня доступа с поддержкой PoE+",
            "price": 85000,
            "currency": "RUB",
            "brand": "Cisco",
            "model": "WS-C2960X-24TS-L",
            "specifications": {
                "Порты": "24x 1Gb Ethernet + 4x SFP",
                "PoE": "PoE+ на всех портах",
                "Пропускная способность": "52 Gbps",
                "Управление": "CLI, Web, SNMP"
            },
            "availability": True
        },
        {
            "name": "HP LaserJet Pro M404n",
            "category": "Принтеры и МФУ",
            "description": "Монохромный лазерный принтер для офиса",
            "price": 15000,
            "currency": "RUB",
            "brand": "HP",
            "model": "LaserJet Pro M404n",
            "specifications": {
                "Тип": "Монохромный лазерный",
                "Скорость печати": "38 стр/мин",
                "Разрешение": "1200x1200 dpi",
                "Подключение": "USB, Ethernet, Wi-Fi"
            },
            "availability": True
        },
        {
            "name": "Samsung Odyssey G7",
            "category": "Мониторы и дисплеи",
            "description": "Игровой монитор с изогнутым экраном 32 дюйма",
            "price": 45000,
            "currency": "RUB",
            "brand": "Samsung",
            "model": "LC32G75TQSRXCI",
            "specifications": {
                "Диагональ": "32 дюйма",
                "Разрешение": "2560x1440 (QHD)",
                "Частота обновления": "240 Гц",
                "Тип панели": "VA",
                "Изгиб": "1000R"
            },
            "availability": True
        },
        {
            "name": "Intel Core i7-13700K",
            "category": "Комплектующие",
            "description": "Процессор Intel 13-го поколения с 16 ядрами",
            "price": 35000,
            "currency": "RUB",
            "brand": "Intel",
            "model": "Core i7-13700K",
            "specifications": {
                "Ядра": "8P + 8E (16 ядер)",
                "Потоки": "24",
                "Базовая частота": "3.4 ГГц",
                "Максимальная частота": "5.4 ГГц",
                "TDP": "125W",
                "Сокет": "LGA1700"
            },
            "availability": True
        },
        {
            "name": "Logitech MX Master 3S",
            "category": "Периферия",
            "description": "Беспроводная мышь для профессионалов",
            "price": 8500,
            "currency": "RUB",
            "brand": "Logitech",
            "model": "MX Master 3S",
            "specifications": {
                "Тип": "Беспроводная",
                "Датчик": "Darkfield 8000 DPI",
                "Батарея": "До 70 дней",
                "Подключение": "Bluetooth, USB-A",
                "Кнопки": "7 программируемых"
            },
            "availability": True
        }
    ]
    
    async with async_session() as db:
        equipment_service = EquipmentService(db)
        
        print("📦 Добавление примеров оборудования...")
        
        for item_data in sample_equipment:
            equipment = EquipmentCreate(**item_data)
            created = await equipment_service.create_equipment(equipment)
            print(f"✅ Добавлено: {created.name} - {created.price:,.0f} {created.currency}")
        
        print(f"\n🎉 Добавлено {len(sample_equipment)} единиц оборудования!")

async def add_admin_user():
    """Добавление администратора"""
    await init_db()
    
    async with async_session() as db:
        user_service = UserService(db)
        
        # Создаем администратора
        admin_data = UserCreate(
            telegram_id=Config.ADMIN_USER_ID,
            username="admin",
            first_name="Admin",
            last_name="User",
            is_admin=True
        )
        
        try:
            admin = await user_service.create_user(admin_data)
            print(f"✅ Создан администратор: {admin.first_name} (ID: {admin.telegram_id})")
        except Exception as e:
            print(f"ℹ️ Администратор уже существует или ошибка: {e}")

async def main():
    """Главная функция"""
    print("🚀 Инициализация базы данных с примерами...")
    
    await add_sample_equipment()
    await add_admin_user()
    
    print("\n✨ Инициализация завершена!")
    print("Теперь вы можете запустить бота командой: python main.py")

if __name__ == "__main__":
    asyncio.run(main())

