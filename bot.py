import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from database import init_db, async_session
from services import EquipmentService, UserService
from models import SearchRequest
from config import Config
import json

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class EquipmentBot:
    def __init__(self):
        self.application = Application.builder().token(Config.BOT_TOKEN).build()
        self.setup_handlers()
    
    def setup_handlers(self):
        """Настройка обработчиков команд"""
        # Команды
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("search", self.search_command))
        self.application.add_handler(CommandHandler("categories", self.categories_command))
        self.application.add_handler(CommandHandler("admin", self.admin_command))
        
        # Обработчики сообщений
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        
        # Регистрируем пользователя в базе данных
        async with async_session() as db:
            user_service = UserService(db)
            await user_service.get_or_create_user(
                telegram_id=user.id,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name
            )
        
        welcome_text = f"""
🔧 Добро пожаловать в бот-консультант по оборудованию!

Привет, {user.first_name}! Я помогу вам найти информацию о стоимости и характеристиках оборудования.

📋 Доступные команды:
/search - Поиск оборудования
/categories - Просмотр категорий
/help - Справка по командам

💡 Просто напишите название оборудования или его характеристики, и я найду подходящие варианты!
        """
        
        keyboard = [
            [InlineKeyboardButton("🔍 Поиск оборудования", callback_data="search")],
            [InlineKeyboardButton("📂 Категории", callback_data="categories")],
            [InlineKeyboardButton("❓ Помощь", callback_data="help")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_text = """
📖 Справка по использованию бота:

🔍 **Поиск оборудования:**
• Используйте команду /search для поиска
• Или просто напишите название оборудования
• Можно указать бренд, модель, характеристики

📂 **Категории оборудования:**
• Компьютеры и ноутбуки
• Серверное оборудование
• Сетевое оборудование
• Принтеры и МФУ
• Мониторы и дисплеи
• Комплектующие
• Периферия
• Другое

💡 **Примеры запросов:**
• "iPhone 15"
• "ноутбук Dell"
• "принтер HP"
• "монитор 24 дюйма"
• "сервер Dell PowerEdge"

⚙️ **Админ-команды** (только для администраторов):
• /admin - Панель администратора
        """
        
        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)
    
    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /search"""
        query = " ".join(context.args) if context.args else ""
        
        if not query:
            await update.message.reply_text(
                "🔍 Введите запрос для поиска оборудования.\n\nПример: /search iPhone 15"
            )
            return
        
        await self.perform_search(update, context, query)
    
    async def categories_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /categories"""
        keyboard = []
        for category in Config.EQUIPMENT_CATEGORIES:
            keyboard.append([InlineKeyboardButton(
                f"📂 {category}", 
                callback_data=f"category_{category}"
            )])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "📂 Выберите категорию оборудования:",
            reply_markup=reply_markup
        )
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /admin"""
        user_id = update.effective_user.id
        
        # Проверяем права администратора
        async with async_session() as db:
            user_service = UserService(db)
            is_admin = await user_service.is_admin(user_id)
        
        if not is_admin and user_id != Config.ADMIN_USER_ID:
            await update.message.reply_text("❌ У вас нет прав администратора.")
            return
        
        keyboard = [
            [InlineKeyboardButton("➕ Добавить оборудование", callback_data="admin_add")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("🌐 Веб-панель", callback_data="admin_web")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "⚙️ Панель администратора:",
            reply_markup=reply_markup
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        query = update.message.text.strip()
        await self.perform_search(update, context, query)
    
    async def perform_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
        """Выполнение поиска оборудования"""
        async with async_session() as db:
            equipment_service = EquipmentService(db)
            search_request = SearchRequest(query=query)
            results = await equipment_service.search_equipment(search_request, limit=10)
        
        if not results:
            await update.message.reply_text(
                f"😔 По запросу '{query}' ничего не найдено.\n\n"
                "Попробуйте:\n"
                "• Изменить поисковый запрос\n"
                "• Использовать более общие термины\n"
                "• Проверить правильность написания"
            )
            return
        
        # Отправляем результаты
        if len(results) == 1:
            await self.send_equipment_details(update, results[0])
        else:
            await self.send_search_results(update, results, query)
    
    async def send_search_results(self, update: Update, results, query: str):
        """Отправка результатов поиска"""
        text = f"🔍 Найдено {len(results)} результатов по запросу '{query}':\n\n"
        
        keyboard = []
        for i, equipment in enumerate(results[:10]):  # Показываем максимум 10 результатов
            price_text = f"{equipment.price:,.0f} {equipment.currency}"
            text += f"{i+1}. **{equipment.name}**\n"
            text += f"   💰 {price_text}\n"
            if equipment.brand:
                text += f"   🏷️ {equipment.brand}"
                if equipment.model:
                    text += f" {equipment.model}"
                text += "\n"
            text += f"   📂 {equipment.category}\n\n"
            
            keyboard.append([InlineKeyboardButton(
                f"{i+1}. {equipment.name[:30]}...",
                callback_data=f"equipment_{equipment.id}"
            )])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    
    async def send_equipment_details(self, update: Update, equipment):
        """Отправка детальной информации об оборудовании"""
        text = f"🔧 **{equipment.name}**\n\n"
        
        price_text = f"{equipment.price:,.0f} {equipment.currency}"
        text += f"💰 **Цена:** {price_text}\n"
        text += f"📂 **Категория:** {equipment.category}\n"
        
        if equipment.brand:
            text += f"🏷️ **Бренд:** {equipment.brand}\n"
        if equipment.model:
            text += f"📱 **Модель:** {equipment.model}\n"
        
        if equipment.description:
            text += f"\n📝 **Описание:**\n{equipment.description}\n"
        
        if equipment.specifications:
            try:
                specs = json.loads(equipment.specifications)
                if specs:
                    text += "\n⚙️ **Характеристики:**\n"
                    for key, value in specs.items():
                        text += f"• {key}: {value}\n"
            except:
                pass
        
        availability = "✅ В наличии" if equipment.availability else "❌ Нет в наличии"
        text += f"\n📦 **Наличие:** {availability}"
        
        keyboard = [
            [InlineKeyboardButton("🔍 Поиск похожих", callback_data=f"similar_{equipment.id}")],
            [InlineKeyboardButton("📂 Категория", callback_data=f"category_{equipment.category}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback запросов"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "search":
            await query.edit_message_text(
                "🔍 Введите название оборудования для поиска:"
            )
        elif data == "categories":
            await self.categories_command(update, context)
        elif data == "help":
            await self.help_command(update, context)
        elif data.startswith("equipment_"):
            equipment_id = int(data.split("_")[1])
            await self.show_equipment_details(query, equipment_id)
        elif data.startswith("category_"):
            category = data.split("_", 1)[1]
            await self.show_category_equipment(query, category)
        elif data.startswith("admin_"):
            await self.handle_admin_callback(query, data)
    
    async def show_equipment_details(self, query, equipment_id: int):
        """Показать детали оборудования"""
        async with async_session() as db:
            equipment_service = EquipmentService(db)
            equipment = await equipment_service.get_equipment(equipment_id)
        
        if not equipment:
            await query.edit_message_text("❌ Оборудование не найдено.")
            return
        
        # Создаем новый update объект для отправки сообщения
        update = Update(update_id=0, message=query.message)
        await self.send_equipment_details(update, equipment)
    
    async def show_category_equipment(self, query, category: str):
        """Показать оборудование категории"""
        async with async_session() as db:
            equipment_service = EquipmentService(db)
            search_request = SearchRequest(category=category)
            results = await equipment_service.search_equipment(search_request, limit=10)
        
        if not results:
            await query.edit_message_text(f"😔 В категории '{category}' пока нет оборудования.")
            return
        
        text = f"📂 Оборудование в категории '{category}':\n\n"
        
        keyboard = []
        for i, equipment in enumerate(results[:10]):
            price_text = f"{equipment.price:,.0f} {equipment.currency}"
            text += f"{i+1}. **{equipment.name}** - {price_text}\n"
            
            keyboard.append([InlineKeyboardButton(
                f"{i+1}. {equipment.name[:30]}...",
                callback_data=f"equipment_{equipment.id}"
            )])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    
    async def handle_admin_callback(self, query, data: str):
        """Обработка админских callback'ов"""
        if data == "admin_add":
            await query.edit_message_text(
                "➕ Для добавления оборудования используйте веб-панель администратора:\n"
                f"http://{Config.ADMIN_PANEL_HOST}:{Config.ADMIN_PANEL_PORT}/admin"
            )
        elif data == "admin_stats":
            await self.show_admin_stats(query)
        elif data == "admin_web":
            await query.edit_message_text(
                f"🌐 Веб-панель администратора:\n"
                f"http://{Config.ADMIN_PANEL_HOST}:{Config.ADMIN_PANEL_PORT}/admin"
            )
    
    async def show_admin_stats(self, query):
        """Показать статистику"""
        async with async_session() as db:
            equipment_service = EquipmentService(db)
            all_equipment = await equipment_service.get_all_equipment()
            categories = await equipment_service.get_categories()
            brands = await equipment_service.get_brands()
        
        total_equipment = len(all_equipment)
        available_equipment = len([e for e in all_equipment if e.availability])
        
        text = f"📊 **Статистика базы данных:**\n\n"
        text += f"🔧 Всего оборудования: {total_equipment}\n"
        text += f"✅ В наличии: {available_equipment}\n"
        text += f"❌ Нет в наличии: {total_equipment - available_equipment}\n"
        text += f"📂 Категорий: {len(categories)}\n"
        text += f"🏷️ Брендов: {len(brands)}\n\n"
        
        if categories:
            text += "📂 **Категории:**\n"
            for category in categories[:5]:  # Показываем первые 5
                count = len([e for e in all_equipment if e.category == category])
                text += f"• {category}: {count}\n"
        
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
    
    async def run(self):
        """Запуск бота"""
        # Инициализация базы данных
        await init_db()
        
        # Запуск бота
        logger.info("Запуск Telegram бота...")
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        logger.info("Бот запущен и готов к работе!")
        
        # Ожидание завершения
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Получен сигнал завершения...")
        finally:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()

if __name__ == "__main__":
    bot = EquipmentBot()
    asyncio.run(bot.run())

