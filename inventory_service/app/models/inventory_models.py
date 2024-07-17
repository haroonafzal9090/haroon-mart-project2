from typing import List, Optional
from sqlmodel import Field, Relationship, SQLModel


class InventoryItem(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    product_id: int
    quantity: int
    status: str 


