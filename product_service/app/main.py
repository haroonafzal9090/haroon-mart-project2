# main.py
from contextlib import asynccontextmanager
import asyncio
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer
from sqlmodel import SQLModel, Session, select
from app.models.product_models import Product, ProductUpdate
from app.dependency import get_session, get_kafka_producer
from app.db import engine
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Annotated
from fastapi import FastAPI, Depends, HTTPException
from app.crud.product_crud import get_all_products, get_product_by_id, delete_product_by_id, update_product_by_id
from app import product_pb2
from app.consumers.product_consumer import consume_messages
from app.consumers.inventory_cosumer import consume_inventory_messages


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("LifeSpan Event..")
    task = asyncio.create_task(consume_messages("products", bootstrap_servers='broker:19092'))
    asyncio.create_task(consume_inventory_messages(
        "AddStock",
        bootstrap_servers='broker:19092'
    ))
    create_db_and_tables()
    yield


app = FastAPI(lifespan=lifespan, title="Hello World API with DB",
              version="0.0.1",
              root_path="/product_service"
              )


@app.get("/")
def read_root():
    return {"App": "Product Service"} 


@app.get("/health")
def health():
    return {"status": "UP"}

@app.post("/manage-products/", response_model=Product)
async def create_new_product(product: Product, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]) -> Product:

    product_protbuf = product_pb2.Product(id=product.id, name=product.name, description=product.description, price=product.price, expiry=product.expiry, brand=product.brand, weight=product.weight, category=product.category, sku=product.sku)
    print(f"Product Protobuf: {product_protbuf}")
    # Serialize the message to a byte string
    serialized_product = product_protbuf.SerializeToString()
    print(f"Serialized data: {serialized_product}")
    # Produce message
    await producer.send_and_wait("products", serialized_product)

    return product


@app.get("/manage-products", response_model=list[Product])
def read_all_products(session: Annotated[Session, Depends(get_session)]):
    return get_all_products(session)

@app.get("/manage-products/{product_id}", response_model=Product)
def get_single_product(product_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single product by ID"""
    try:
        return get_product_by_id(product_id=product_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.delete("/manage-products/{product_id}", response_model=dict)
def delete_single_product(product_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Delete a single product by ID"""
    try:
        return delete_product_by_id(product_id=product_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.patch("/manage-products/{product_id}", response_model=Product)
def update_single_product(product_id: int, product: ProductUpdate, session: Annotated[Session, Depends(get_session)]):
    """ Update a single product by ID"""
    try:
        return update_product_by_id(product_id=product_id, to_update_product_data=product, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))