from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import json
from datetime import datetime

from database import get_db, init_db
from services import EquipmentService, UserService
from models import EquipmentCreate, EquipmentUpdate, SearchRequest
from config import Config

app = FastAPI(title="Equipment Bot Admin Panel")

# Настройка шаблонов
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    await init_db()

@app.get("/", response_class=HTMLResponse)
async def admin_dashboard(request: Request, db: AsyncSession = Depends(get_db)):
    """Главная страница админ-панели"""
    equipment_service = EquipmentService(db)
    
    # Получаем статистику
    all_equipment = await equipment_service.get_all_equipment()
    categories = await equipment_service.get_categories()
    brands = await equipment_service.get_brands()
    
    stats = {
        "total_equipment": len(all_equipment),
        "available_equipment": len([e for e in all_equipment if e.availability]),
        "categories_count": len(categories),
        "brands_count": len(brands),
        "categories": categories,
        "brands": brands[:10]  # Показываем первые 10 брендов
    }
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats
    })

@app.get("/equipment", response_class=HTMLResponse)
async def equipment_list(
    request: Request, 
    page: int = 1,
    search: Optional[str] = None,
    category: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Список оборудования"""
    equipment_service = EquipmentService(db)
    skip = (page - 1) * 20
    
    if search or category:
        search_request = SearchRequest(query=search, category=category)
        equipment = await equipment_service.search_equipment(search_request, skip=skip, limit=20)
    else:
        equipment = await equipment_service.get_all_equipment(skip=skip, limit=20)
    
    categories = await equipment_service.get_categories()
    
    return templates.TemplateResponse("equipment_list.html", {
        "request": request,
        "equipment": equipment,
        "categories": categories,
        "current_page": page,
        "search_query": search,
        "selected_category": category
    })

@app.get("/equipment/add", response_class=HTMLResponse)
async def add_equipment_form(request: Request):
    """Форма добавления оборудования"""
    return templates.TemplateResponse("equipment_form.html", {
        "request": request,
        "equipment": None,
        "categories": Config.EQUIPMENT_CATEGORIES,
        "action": "add"
    })

@app.post("/equipment/add")
async def add_equipment(
    name: str = Form(...),
    category: str = Form(...),
    description: Optional[str] = Form(None),
    price: float = Form(...),
    currency: str = Form("RUB"),
    brand: Optional[str] = Form(None),
    model: Optional[str] = Form(None),
    specifications: Optional[str] = Form(None),
    availability: bool = Form(True),
    db: AsyncSession = Depends(get_db)
):
    """Добавление нового оборудования"""
    equipment_service = EquipmentService(db)
    
    # Парсим спецификации если они есть
    specs = None
    if specifications:
        try:
            specs = json.loads(specifications)
        except:
            pass
    
    equipment_data = EquipmentCreate(
        name=name,
        category=category,
        description=description,
        price=price,
        currency=currency,
        brand=brand,
        model=model,
        specifications=specs,
        availability=availability
    )
    
    await equipment_service.create_equipment(equipment_data)
    return RedirectResponse(url="/equipment", status_code=303)

@app.get("/equipment/{equipment_id}/edit", response_class=HTMLResponse)
async def edit_equipment_form(request: Request, equipment_id: int, db: AsyncSession = Depends(get_db)):
    """Форма редактирования оборудования"""
    equipment_service = EquipmentService(db)
    equipment = await equipment_service.get_equipment(equipment_id)
    
    if not equipment:
        raise HTTPException(status_code=404, detail="Equipment not found")
    
    return templates.TemplateResponse("equipment_form.html", {
        "request": request,
        "equipment": equipment,
        "categories": Config.EQUIPMENT_CATEGORIES,
        "action": "edit"
    })

@app.post("/equipment/{equipment_id}/edit")
async def edit_equipment(
    equipment_id: int,
    name: str = Form(...),
    category: str = Form(...),
    description: Optional[str] = Form(None),
    price: float = Form(...),
    currency: str = Form("RUB"),
    brand: Optional[str] = Form(None),
    model: Optional[str] = Form(None),
    specifications: Optional[str] = Form(None),
    availability: bool = Form(True),
    db: AsyncSession = Depends(get_db)
):
    """Редактирование оборудования"""
    equipment_service = EquipmentService(db)
    
    # Парсим спецификации если они есть
    specs = None
    if specifications:
        try:
            specs = json.loads(specifications)
        except:
            pass
    
    equipment_data = EquipmentUpdate(
        name=name,
        category=category,
        description=description,
        price=price,
        currency=currency,
        brand=brand,
        model=model,
        specifications=specs,
        availability=availability
    )
    
    updated_equipment = await equipment_service.update_equipment(equipment_id, equipment_data)
    if not updated_equipment:
        raise HTTPException(status_code=404, detail="Equipment not found")
    
    return RedirectResponse(url="/equipment", status_code=303)

@app.post("/equipment/{equipment_id}/delete")
async def delete_equipment(equipment_id: int, db: AsyncSession = Depends(get_db)):
    """Удаление оборудования"""
    equipment_service = EquipmentService(db)
    success = await equipment_service.delete_equipment(equipment_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Equipment not found")
    
    return RedirectResponse(url="/equipment", status_code=303)

@app.get("/api/equipment", response_class=HTMLResponse)
async def api_equipment_list(
    search: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """API для получения списка оборудования"""
    equipment_service = EquipmentService(db)
    
    if search or category:
        search_request = SearchRequest(query=search, category=category)
        equipment = await equipment_service.search_equipment(search_request, limit=limit)
    else:
        equipment = await equipment_service.get_all_equipment(limit=limit)
    
    # Возвращаем JSON
    from fastapi.responses import JSONResponse
    return JSONResponse(content=[
        {
            "id": e.id,
            "name": e.name,
            "category": e.category,
            "price": e.price,
            "currency": e.currency,
            "brand": e.brand,
            "model": e.model,
            "availability": e.availability,
            "created_at": e.created_at.isoformat()
        }
        for e in equipment
    ])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "admin_panel:app",
        host=Config.ADMIN_PANEL_HOST,
        port=Config.ADMIN_PANEL_PORT,
        reload=True
    )

