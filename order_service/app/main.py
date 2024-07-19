from sqlmodel import Session, select, SQLModel
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator, Annotated
from app.db import engine
from app.models.order_models import OrderItem
from app.dependency import get_session, get_kafka_producer
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
import asyncio
from app.cosumers.add_order_cosumer import consume_messages
from app.crud.order_crud import get_all_order_items, get_order_item_by_id
from app import order_pb2

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    
    print("Created DB and tables")
    # task = asyncio.create_task(consume_messages(
    #     "order-add-stock-response", 'broker:19092'))
    asyncio.create_task(consume_messages("order-add-stock-response", bootstrap_servers='broker:19092'))

    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan, title="Order Service API",
              version="0.0.1",
              root_path="/order_service"
              )

@app.get("/")
def read_root():
    return {"App": "Order Service"}

@app.get("/health")
def health():
    return {"status": "UP"}


@app.post("/manage-orders/", response_model=OrderItem)
async def create_order_item(order_item: OrderItem, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]) -> OrderItem:

    order_item_protbuf = order_pb2.OrderItem(id=order_item.id, inventory_product_id=order_item.inventory_product_id, quantity=order_item.quantity, status=order_item.status)
    print(f"Order Item Protobuf: {order_item_protbuf}")
    # Serialize the message to a byte string
    serialized_order_item = order_item_protbuf.SerializeToString()
    print(f"Serialized data: {serialized_order_item}")
    # Produce message
    await producer.send_and_wait("AddOrder", serialized_order_item)

    return order_item


@app.get("/manage-orders/", response_model=list[OrderItem])
def read_all_order_items(session: Annotated[Session, Depends(get_session)]):
    return get_all_order_items(session)


@app.get("/manage-orders/{order_id}", response_model=OrderItem)
def get_single_order_item(order_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single order item by ID"""
    try:
        return get_order_item_by_id(order_item_id=order_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
