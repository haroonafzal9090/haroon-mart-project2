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
from app.consumers.order_cosumer import consume_order_messages


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


# async def consume_messages(topic, bootstrap_servers):
#     # Create a consumer instance.
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id="product-consumer-group",
#         auto_offset_reset='earliest'
#     )

#     # Start the consumer.
#     await consumer.start()
#     try:
#         # Continuously listen for messages.
#         async for message in consumer:
#             print(f"Received message: {
#                   message.value.decode()} on topic {message.topic}")
#             # Here you can add code to process each message.
#             # Example: parse the message, store it in a database, etc.
#             new_product = product_pb2.Product()
#             new_product.ParseFromString(message.value)
#             print(f"\n\n Consumer Deserialized data: {new_product}")

#             # Add the new product to the database
#             with Session(engine) as session:
#                 session.add(new_product)
#                 session.commit()
#                 session.refresh(new_product)
#                 print(f"New product added to database: {new_product}")
#     finally:
#         # Ensure to close the consumer when done.
#         await consumer.stop()


# The first part of the function, before the yield, will
# be executed before the application starts.
# https://fastapi.tiangolo.com/advanced/events/#lifespan-function
# loop = asyncio.get_event_loop()
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("LifeSpan Event..")
    task = asyncio.create_task(consume_messages('products', 'broker:19092'))
    asyncio.create_task(consume_order_messages(
        "AddOrder",
        'broker:19092'
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