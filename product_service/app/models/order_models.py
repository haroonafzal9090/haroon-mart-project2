from typing import List, Optional
from sqlmodel import Field, Relationship, SQLModel


class Order(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(index=True)
    quantity: int
