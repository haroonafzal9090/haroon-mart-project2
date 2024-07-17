from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator, Annotated
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlmodel import Session, select, SQLModel
from app.models.inventory_models import InventoryItem
from app.dependency import get_session, get_kafka_producer
from app.db import engine
from app.cosumers.add_inventory_cosumer import consume_messages
from app import inventory_pb2
from app.crud.inventory_crud import get_all_inventory_items, get_inventory_item_by_id, add_new_inventory_item, delete_inventory_item_by_id
import asyncio

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    
    print("Created DB and tables")
    asyncio.create_task(consume_messages(
        "inventory-add-stock-response", bootstrap_servers='broker:19092'))
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan, title="Inventory Service API",
              version="0.0.1",
              root_path="/inventory_service"
              )

@app.get("/")
def read_root():
    return {"App": "Inventory Service"}

@app.get("/health")
def health():
    return {"status": "UP"}


@app.post("/manage-inventory/", response_model=InventoryItem)
async def create_new_inventory_item(inventory_item: InventoryItem, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]) -> InventoryItem:

    inventory_item_protbuf = inventory_pb2.InventoryItem(id=inventory_item.id, product_id=inventory_item.product_id, quantity=inventory_item.quantity, status=inventory_item.status)
    print(f"Inventory Item Protobuf: {inventory_item_protbuf}")
    # Serialize the message to a byte string
    serialized_inventory_item = inventory_item_protbuf.SerializeToString()
    print(f"Serialized data: {serialized_inventory_item}")
    # Produce message
    await producer.send_and_wait("AddStock", serialized_inventory_item)

    return inventory_item


@app.get("/manage-invenory", response_model=list[InventoryItem])
def read_all_inventory_items(session: Annotated[Session, Depends(get_session)]):
    return get_all_inventory_items(session)

@app.get("/manage-inventory/{inventory_id}", response_model=InventoryItem)
def get_single_inventory_item(inventory_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single inventory item by ID"""
    try:
        return get_inventory_item_by_id(inventory_item_id=inventory_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# @app.delete("/manage-products/{product_id}", response_model=dict)
# def delete_single_product(product_id: int, session: Annotated[Session, Depends(get_session)]):
#     """ Delete a single product by ID"""
#     try:
#         return delete_product_by_id(product_id=product_id, session=session)
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
