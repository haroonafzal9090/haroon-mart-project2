from typing import List, Optional
from sqlmodel import Field, Relationship, SQLModel
import uuid


class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(index=True)
    quantity: int



