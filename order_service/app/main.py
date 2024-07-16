from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator, Annotated
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlmodel import Session, select, SQLModel
from app.models.order_models import Order
from app.dependency import get_session, get_kafka_producer
from app.db import engine
from app.cosumers.add_order_cosumer import consume_messages
from app import order_pb2
from app.crud.order_crud import get_all_orders, get_order_item_by_id
import asyncio

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)



# async def consume_messages(topic, bootstrap_servers):
#     # Create a consumer instance.
#     try:
#         consumer = AIOKafkaConsumer(
#             topic,
#             bootstrap_servers=bootstrap_servers,
#             group_id="my-orders-group",
#             auto_offset_reset='earliest'
#         )

#         await consumer.start()
#         print("Kafka consumer started successfully.")

#         async for message in consumer:
#             try:
#                 new_product = product_pb2.Product()
#                 new_product.ParseFromString(message.value)
#                 print(f"Consumer Deserialized data: {new_product}")

#                 # Example: Implement your logic here to process the message
#                 # For now, let's print the product ID
#                 product_id = new_product.id
#                 print(f"Product ID: {product_id}")

                
                



#             except Exception as e:
#                 print(f"Error processing message: {e}")

#     # except KafkaConnectionError as kce:
#     #     print(f"Kafka connection error: {kce}")
#     #     raise  # Re-raise the exception to handle it in your application

#     except Exception as e:
#         print(f"Error consuming messages: {e}")

#     finally:
#         await consumer.stop()
#         print("Kafka consumer stopped.")

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    
    print("Created DB and tables")
    asyncio.create_task(consume_messages(
        "order-add-stock-response", bootstrap_servers='broker:19092'))
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


@app.post("/manage-orders/", response_model=Order)
async def create_new_order(order: Order, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]) -> Order:

    order_protbuf = order_pb2.Order(id=order.id, product_id=order.product_id, quantity=order.quantity)
    print(f"Order Protobuf: {order_protbuf}")
    # Serialize the message to a byte string
    serialized_order = order_protbuf.SerializeToString()
    print(f"Serialized data: {serialized_order}")
    # Produce message
    await producer.send_and_wait("AddOrder", serialized_order)

    return order


@app.get("/manage-orders", response_model=list[Order])
def read_all_orders(session: Annotated[Session, Depends(get_session)]):
    return get_all_orders(session)

@app.get("/manage-orders/{order_id}", response_model=Order)
def get_single_order(order_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single order by ID"""
    try:
        return get_order_item_by_id(order_id=order_id, session=session)
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
