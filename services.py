from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import selectinload
from typing import List, Optional, Dict, Any
import json
from database import Equipment, User
from models import EquipmentCreate, EquipmentUpdate, UserCreate, SearchRequest

class EquipmentService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_equipment(self, equipment_data: EquipmentCreate) -> Equipment:
        """Create new equipment item"""
        db_equipment = Equipment(
            name=equipment_data.name,
            category=equipment_data.category,
            description=equipment_data.description,
            price=equipment_data.price,
            currency=equipment_data.currency,
            brand=equipment_data.brand,
            model=equipment_data.model,
            specifications=json.dumps(equipment_data.specifications) if equipment_data.specifications else None,
            availability=equipment_data.availability
        )
        self.db.add(db_equipment)
        await self.db.commit()
        await self.db.refresh(db_equipment)
        return db_equipment
    
    async def get_equipment(self, equipment_id: int) -> Optional[Equipment]:
        """Get equipment by ID"""
        result = await self.db.execute(select(Equipment).where(Equipment.id == equipment_id))
        return result.scalar_one_or_none()
    
    async def get_all_equipment(self, skip: int = 0, limit: int = 100) -> List[Equipment]:
        """Get all equipment with pagination"""
        result = await self.db.execute(
            select(Equipment).offset(skip).limit(limit).order_by(Equipment.created_at.desc())
        )
        return result.scalars().all()
    
    async def search_equipment(self, search_request: SearchRequest, skip: int = 0, limit: int = 50) -> List[Equipment]:
        """Search equipment with filters"""
        query = select(Equipment)
        conditions = []
        
        if search_request.query:
            search_term = f"%{search_request.query}%"
            conditions.append(
                or_(
                    Equipment.name.ilike(search_term),
                    Equipment.description.ilike(search_term),
                    Equipment.brand.ilike(search_term),
                    Equipment.model.ilike(search_term)
                )
            )
        
        if search_request.category:
            conditions.append(Equipment.category == search_request.category)
        
        if search_request.min_price is not None:
            conditions.append(Equipment.price >= search_request.min_price)
        
        if search_request.max_price is not None:
            conditions.append(Equipment.price <= search_request.max_price)
        
        if search_request.brand:
            conditions.append(Equipment.brand.ilike(f"%{search_request.brand}%"))
        
        if search_request.availability is not None:
            conditions.append(Equipment.availability == search_request.availability)
        
        if conditions:
            query = query.where(and_(*conditions))
        
        query = query.offset(skip).limit(limit).order_by(Equipment.created_at.desc())
        result = await self.db.execute(query)
        return result.scalars().all()
    
    async def update_equipment(self, equipment_id: int, equipment_data: EquipmentUpdate) -> Optional[Equipment]:
        """Update equipment"""
        db_equipment = await self.get_equipment(equipment_id)
        if not db_equipment:
            return None
        
        update_data = equipment_data.dict(exclude_unset=True)
        if "specifications" in update_data and update_data["specifications"]:
            update_data["specifications"] = json.dumps(update_data["specifications"])
        
        for field, value in update_data.items():
            setattr(db_equipment, field, value)
        
        await self.db.commit()
        await self.db.refresh(db_equipment)
        return db_equipment
    
    async def delete_equipment(self, equipment_id: int) -> bool:
        """Delete equipment"""
        db_equipment = await self.get_equipment(equipment_id)
        if not db_equipment:
            return False
        
        await self.db.delete(db_equipment)
        await self.db.commit()
        return True
    
    async def get_categories(self) -> List[str]:
        """Get all unique categories"""
        result = await self.db.execute(select(Equipment.category).distinct())
        return [row[0] for row in result.fetchall()]
    
    async def get_brands(self) -> List[str]:
        """Get all unique brands"""
        result = await self.db.execute(
            select(Equipment.brand).where(Equipment.brand.isnot(None)).distinct()
        )
        return [row[0] for row in result.fetchall()]

class UserService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_user(self, user_data: UserCreate) -> User:
        """Create new user"""
        db_user = User(
            telegram_id=user_data.telegram_id,
            username=user_data.username,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            is_admin=user_data.is_admin
        )
        self.db.add(db_user)
        await self.db.commit()
        await self.db.refresh(db_user)
        return db_user
    
    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        """Get user by Telegram ID"""
        result = await self.db.execute(select(User).where(User.telegram_id == telegram_id))
        return result.scalar_one_or_none()
    
    async def get_or_create_user(self, telegram_id: int, username: str = None, 
                                first_name: str = None, last_name: str = None) -> User:
        """Get existing user or create new one"""
        user = await self.get_user_by_telegram_id(telegram_id)
        if user:
            # Update user info if changed
            if username and user.username != username:
                user.username = username
            if first_name and user.first_name != first_name:
                user.first_name = first_name
            if last_name and user.last_name != last_name:
                user.last_name = last_name
            await self.db.commit()
            return user
        
        # Create new user
        user_data = UserCreate(
            telegram_id=telegram_id,
            username=username,
            first_name=first_name,
            last_name=last_name
        )
        return await self.create_user(user_data)
    
    async def is_admin(self, telegram_id: int) -> bool:
        """Check if user is admin"""
        user = await self.get_user_by_telegram_id(telegram_id)
        return user.is_admin if user else False

