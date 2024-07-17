from typing import List, Optional
from sqlmodel import Field, Relationship, SQLModel


class OrderItem(SQLModel):
    id: int | None = Field(default=None, primary_key=True)
    inventory_product_id: int
    quantity: int
    status: str 

